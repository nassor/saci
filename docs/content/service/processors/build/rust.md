+++
title = "A Rust processor"
description = "The only language with an SDK: a plain wasm32-wasip2 cargo build, saci-processor, export_pipeline!, and a real component running under saci-service at the end."
template = "page.html"
weight = 1
aliases = ["/processors/rust/", "/guests/rust/"]
+++
# A Rust processor

`settle-rs.wasm` is a WebAssembly component that reads an `Order` batch, writes
its `settlement` column, and keeps a running ledger across batches. It runs
under `saci-service` against a six-row CSV fixture and writes a CSV you can
read.

Every block is from `examples/polyglot/stages/rust-settle/`, stage six of the
polyglot example, which CI builds and asserts.

<div class="dgm animate-in">
    <div class="dgm-scroll"><svg viewBox="0 0 660 176" role="img" aria-labelledby="rs-title rs-desc">
        <title id="rs-title">The settle stage reads a CSV, writes the settlement column, and carries a ledger across batches</title>
        <desc id="rs-desc">
            saci-service reads six order rows from examples/configs/fixtures/polyglot_orders.csv
            and hands them to the settle-rs WebAssembly component. The component writes the
            settlement column of every row and returns the batch, which the file sink writes to
            /tmp/saci-polyglot-out.csv. A second arrow loops out of the component and back into
            it: the ledger it returns as a checkpoint comes back as the next batch's prior, so
            settled volume accumulates across batches while nothing else does.
        </desc>
        <g class="anim anim-1">
            <rect class="blk blk-data" x="0" y="40" width="190" height="60" rx="8"/>
            <rect class="hd hd-data" x="0" y="40" width="190" height="20" rx="8"/>
            <rect class="hd hd-data" x="0" y="52" width="190" height="8"/>
            <text class="t-lbl" x="12" y="55">polyglot_orders.csv</text>
            <text class="t-sm" x="12" y="76">six Order rows</text>
            <text class="t-sm" x="12" y="90">settlement reads PENDING</text>
        </g>
        <g class="anim anim-2">
            <text class="t-sm t-mid" x="222" y="62">Arrow IPC</text>
            <path class="arw arw-data" d="M190 70 H252" marker-end="url(#rs-d)"/>
            <rect class="blk blk-bnd" x="258" y="40" width="150" height="60" rx="8"/>
            <rect class="hd hd-bnd" x="258" y="40" width="150" height="20" rx="8"/>
            <rect class="hd hd-bnd" x="258" y="52" width="150" height="8"/>
            <text class="t-lbl" x="270" y="55">wasm settle-rs</text>
            <text class="t-sm t-bnd" x="270" y="76">writes settlement</text>
            <text class="t-sm" x="270" y="90">SETTLED or REJECTED</text>
        </g>
        <g class="anim anim-3">
            <text class="t-sm t-mid" x="440" y="62">Arrow IPC</text>
            <path class="arw arw-data" d="M408 70 H470" marker-end="url(#rs-d)"/>
            <rect class="blk blk-data" x="476" y="40" width="184" height="60" rx="8"/>
            <rect class="hd hd-data" x="476" y="40" width="184" height="20" rx="8"/>
            <rect class="hd hd-data" x="476" y="52" width="184" height="8"/>
            <text class="t-lbl" x="488" y="55">saci-polyglot-out.csv</text>
            <text class="t-sm" x="488" y="76">same twelve columns</text>
            <text class="t-sm" x="488" y="90">settlement filled in</text>
        </g>
        <g class="anim anim-4">
            <path class="arw arw-bnd" d="M290 100 V136 H376 V100" marker-end="url(#rs-b)"/>
            <text class="t-sm t-bnd t-mid" x="333" y="152">ledger checkpoint, back as the next prior</text>
        </g>
        <defs>
            <marker id="rs-d" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--data-ink)"/>
            </marker>
            <marker id="rs-b" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="6" markerHeight="6" orient="auto">
                <path d="M0 0 L8 4 L0 8 z" fill="var(--boundary-ink)"/>
            </marker>
        </defs>
    </svg>
    </div>
    <div class="dgm-key">
        <span class="k-data"><i></i> the batch: Arrow IPC bytes in, Arrow IPC bytes out</span>
        <span class="k-boundary"><i></i> the component, and the ledger that outlives its batch</span>
    </div>
    <figcaption class="dgm-cap">
        The two horizontal arrows are one batch. The loop under the component is the
        only thing that crosses a batch boundary: <code>settlement</code> goes downstream
        to the sink, the <code>Ledger</code> does not.
    </figcaption>
</div>

## What you need

Rust 1.95 or newer, and the `wasm32-wasip2` target. `rustc` links a
`wasm32-wasip2` cdylib into a Component Model component itself, so there is no
componentizer step and no adapter. `cargo build` is the whole toolchain.

```bash,name=Add the wasm32-wasip2 target
rustup target add wasm32-wasip2
```
Runs the same on Linux, macOS and Windows (PowerShell).

Rust needs no separate SDK package. `saci-processor` 0.1.0 is the one dependency:
it carries the Arrow handling itself, re-exports `arrow_array` and
`arrow_schema` at the version the host uses, and provides the four authoring
macros. The other five languages install a `saci-sdk-*` package for exactly this.

## 1. Create the project

A processor is a `cdylib`, not a binary. `crate-type = ["cdylib"]` is what makes
`cargo build --target wasm32-wasip2` emit a component rather than a library.

```toml,name=Cargo.toml
[package]
name = "polyglot-settle-wasm"
version = "0.1.0"
edition = "2024"
rust-version = "1.95.0"
publish = false

[lib]
crate-type = ["cdylib"]

[dependencies]
saci-processor = "0.1.0"
# `#[derive(Component)]` requires `Serialize`/`Deserialize` alongside it, and
# serde's own expansion names the literal `serde` crate at the call site, so it
# is a direct dependency rather than a re-export.
serde = { version = "1.0.229", features = ["derive"] }

# The bindings generator, invoked by the `#[processor]` expansion. Gated on
# wasm32 because that expansion is: the host build of this crate is an empty
# cdylib and has no use for the generator or its proc-macro dependency tree.
[target.'cfg(target_arch = "wasm32")'.dependencies]
wit-bindgen = "0.62.0"
```

The in-repo stage points `saci-processor` at
`../../../../crates/saci-processor` by path instead of by version. Everything
else is identical.

## 2. Declare the row type

The row type is the schema. `#[derive(Component)]` traces the Arrow schema off
the Rust types, so nothing declares a field twice and nothing can drift.

```rust,name=The Order row in src/lib.rs
use saci_processor::prelude::*;

#[derive(Component, serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct Order {
    pub id: i64,
    pub region: String,
    pub currency: String,
    pub amount: f64,
    pub valid: bool,
    pub usd_amount: f64,
    pub usd_amount_display: String,
    pub risk_score: f64,
    pub flagged: bool,
    pub fee: f64,
    pub review_tier: i64,
    pub settlement: String,
}
```

Four Rust types cover the twelve fields: `i64` becomes Arrow `Int64`, `f64`
becomes `Float64`, `bool` becomes `Boolean`, and `String` becomes `Utf8`.
Declaration order is wire order, so the field sequence above is the column
sequence in the batch and in the CSV.

Two stages agree when they declare the same field names in the same order.
[The wire format](@/library/reference/wire-format.md) specifies the algorithm
that turns that declaration into the fingerprint both stages report.

Every column exists from the start, including the ones another stage writes. A
processor is handed a batch and hands one back; it does not add columns. The
`settlement` field is this stage's output, and the eleven before it are input or
another stage's output.

## 3. Write the transform

`#[transform]` wraps a function that sees one row at a time. It is handed a
`&mut Order` and writes into it.

```rust,name=The settle transform
#[transform(component = Order)]
pub fn settle(row: &mut Order) -> Result<()> {
    row.settlement = if !row.valid {
        REJECTED.to_string()
    } else {
        match row.review_tier {
            TIER_HOLD => HOLD.to_string(),
            TIER_REVIEW => REVIEW.to_string(),
            TIER_CLEAR => SETTLED.to_string(),
            other => {
                return Err(format!(
                    "polyglot-settle: row {} carries unknown review_tier {other}",
                    row.id
                )
                .into());
            }
        }
    };
    Ok(())
}
```

The four outcomes are `REJECTED`, `HOLD`, `REVIEW` and `SETTLED`, and the three
tiers are `2`, `1` and `0`. Rejection wins over the tier. A row an upstream
stage rejected never had its amount converted, so its score and therefore its
tier mean nothing.

Returning `Err` fails the whole batch. That is the right answer for an
unrecognised tier, which is an upstream fault rather than a row to settle by
default.

## 4. Export it

One attribute on one function is the whole export. `#[processor]` emits the WIT
world, the guest exports and the host-io bridges, so nothing in this file is
target-gated by hand.

```rust,name=The processor entry point
#[processor(name = "polyglot-settle-rs", state = Ledger)]
pub fn build() -> Pipeline {
    Pipeline::builder("polyglot-settle-rs")
        .with::<Order>()
        .with_system(settle_system())
        .with_system(ledger_system())
        .build()
}
```

`settle_system()` and `ledger_system()` are what `#[transform]` and `#[fold]`
generate from the two functions: one constructor per annotated function, named
after it. `with::<Order>()` registers the row type, which is what a workflow's
`component="Order"` is checked against.

`state = Ledger` names the type carried across batches, declared in
[Config, logs and state](#config-logs-and-state). Only `Order` is registered.
Registering `Ledger` would be wrong. A registered component holds exactly the
dataset's row count, while ledger rows are independent of batch rows.

## 5. Build and validate

```bash,name=Build the component
cargo build --release -p polyglot-settle-wasm --target wasm32-wasip2
```
Runs the same on Linux, macOS and Windows (PowerShell).

The finished component is
`target/wasm32-wasip2/release/polyglot_settle_wasm.wasm`. Nothing else lands on
disk: the bindings live inside the macro expansion, there is no generated source
file, and there is no preview 1 core module to convert.

`cargo xtask polyglot` runs that same build and copies the artifact to
`examples/polyglot/build/settle-rs.wasm`, which is the path the config below
names.

Confirm the artifact is a valid component that exports the right world with
[the two verify commands](@/service/processors/build/_index.md) on the build
hub.

## 6. Run it under saci-service

`examples/configs/standalone_polyglot.kdl` runs one polyglot stage against a
six-row CSV fixture. It ships pointed at the Python stage, so two things change
for this one. First, the `wasm` node names this component:

```kdl,name=The wasm node for the settle stage
wasm "settle" module="examples/polyglot/build/settle-rs.wasm"
```

Point the workflow's two `link` lines at `settle` as well. Second, the node
carries no `config` child. `settle` takes no config keys, so there is nothing
to inject.

The fixture pre-seeds `settlement` as `PENDING` rather than leaving it blank.
The csv transformer turns an empty field into a null, and the column is
declared non-nullable, so a blank field fails the batch before any row is
ingested.

Run it from the repository root:

```bash,name=Validate the config then serve it
cargo run -p saci-service --features connector-file,transformer-csv,wasm -- validate \
  --config examples/configs/standalone_polyglot.kdl --strict

cargo run -p saci-service --features connector-file,transformer-csv,wasm -- serve \
  --config examples/configs/standalone_polyglot.kdl
```
Windows (PowerShell):

```powershell
cargo run -p saci-service --features connector-file,transformer-csv,wasm -- validate --config examples/configs/standalone_polyglot.kdl --strict
cargo run -p saci-service --features connector-file,transformer-csv,wasm -- serve --config examples/configs/standalone_polyglot.kdl
```

`validate` reads the config, compiles the component and checks that every source
and sink names a component the processor declares. Nothing reads a row until it
passes. `run_mode` is `one_shot`, so `serve` processes the fixture once and
exits.

The column this stage fills is `settlement`, in
`/tmp/saci-polyglot-out.csv`. All twelve columns are there; these two are the
readable check:

```text,name=The id and settlement columns of the CSV the sink wrote
id,settlement
1,SETTLED
2,REJECTED
3,SETTLED
4,SETTLED
5,REJECTED
6,SETTLED
```

Rows 2 and 5 are the two the fixture seeds as `valid=false`. The other four
carry `review_tier` 0, so they settle. Run the full six-stage chain and the
tiers stop being zero, which turns some of those into `REVIEW` or `HOLD`.

## Config, logs and state

A `#[transform]` that takes a second `&Config` parameter is handed typed access
to the `config` keys the `wasm` node injected. `Config` has a single method. An
absent key yields the default, and a present but unparseable value is an error,
because falling back silently would hide an operator's typo behind
working-looking behaviour.

```rust,name=Reading a config key with a default
#[transform(component = Order)]
pub fn settle(row: &mut Order, config: &Config) -> Result<()> {
    let floor: f64 = config.get("min_amount", 0.0)?;
    row.valid = row.amount > floor;
    Ok(())
}
```

`println!` from a processor goes nowhere. The host gives the component no
stdout and no stderr, so both are discarded. `log(target, message)` and
`metric(name, value)`, emitted into your crate by `#[processor]`, are the only
channels out. The host records each metric under the processor's own label.

`#[fold]` is the batch-level hook. It sees the whole row slice and a `&mut` to
the state type, which is the only place a count over the batch can live.

```rust,name=The ledger fold and its state
#[derive(Component, serde::Serialize, serde::Deserialize, Clone, Debug, Default, PartialEq)]
pub struct Ledger {
    pub settled_count: i64,
    pub settled_usd: f64,
}

#[fold(reads = Order, state = Ledger)]
pub fn ledger(rows: &[Order], state: &mut Ledger) -> Result<()> {
    let mut batch_count = 0i64;
    let mut batch_usd = 0.0f64;

    for row in rows {
        if row.settlement == SETTLED {
            batch_count += 1;
            batch_usd += row.usd_amount - row.fee;
        }
    }

    state.settled_count += batch_count;
    state.settled_usd += batch_usd;

    metric("settle.settled_usd_total", state.settled_usd);
    metric("settle.settled_count_total", state.settled_count as f64);
    log(
        "ledger",
        &format!(
            "batch settled {batch_count} rows / {batch_usd:.2} USD net; \
             lifetime {} rows / {:.2} USD net",
            state.settled_count, state.settled_usd
        ),
    );
    Ok(())
}
```

The state blob a processor returns is the only thing that survives to the next
batch. `#[processor(state = Ledger)]` serialises `Ledger` into the checkpoint
after every batch and restores it from the next call's prior, so
`settled_count` and `settled_usd` accumulate while a global or a struct field
would not. `Default` is the cold start: zero rows settled, zero volume.

State never reaches the output. It lives beside the batch rather than in it, so
it does not round-trip through Arrow and never appears in the CSV or in the
descriptor the processor reports.

`ledger` writes no column. It reads `Order`, which `settle` writes, and that
alone places it after `settle`. The sequencing comes from the declared access,
not from an ordering call. On the single-stage run above, `usd_amount` and `fee`
are still zero, so its log line reads
`batch settled 4 rows / 0.00 USD net; lifetime 4 rows / 0.00 USD net`.

## Next

- [The WIT contract](@/service/processors/build/wit-contract.md): every field of
  the descriptor, and what it is checked against.
- [Operating saci-service](@/service/operate/_index.md): deploying a built
  `.wasm`.
