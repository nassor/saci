# Polyglot: one pipeline, six languages

A single SACI workload implemented as six WebAssembly components, written in
Go, Python, TypeScript, Kotlin, C# and Rust. All six export the same
`saci:pipeline@0.3.0` WIT world, all operate on the same `Order` component, and
each stage reads the fields an earlier one wrote and writes the next set. The
WIT world is the contract and the Rust SDK is one way to meet it: any language
that compiles to a WASI 0.2 component can be a SACI stage.

## Prerequisites

All toolchain versions below are pinned in `PINS.md` in this directory; the
platform caveats live there too. `cargo xtask polyglot` checks every tool the
requested stages need before building anything.

- Rust 1.95.0 with the `wasm32-wasip2` target: `rustup target add wasm32-wasip2`
- `wasm-tools` 1.246.2
- Go 1.25.5+ with `componentize-go` 0.4.1
- Python 3.10+ with `componentize-py` 0.25.0
- Node 24.12+ with `@bytecodealliance/jco` 1.30.0 and `typescript` 5.9.3
- JDK 21, Gradle 8.14.4+, and the Kotlin `wit-bindgen` fork (branch `kotlin`,
  reports 0.57.1); Kotlin 2.4.0 itself is fetched by Gradle
- The .NET SDK 10 (verified on 10.0.400); no `dotnet workload` step, but the
  first C# build downloads wasi-sdk 29.0, about 535 MB, into `~/.wasi-sdk/`

The five non-Rust stages each consume one `saci-sdk-*` package from
`packages/` at the repository root. Each SDK carries its language's Arrow IPC
codec internally, so one coordinate resolves both.

## The chain

`Order` has twelve fields. Each stage reads what an earlier one wrote:

| # | stage | language | reads | writes |
|---|-------|----------|-------|--------|
| 1 | `polyglot-validate-go` | Go | `amount` | `valid` |
| 2 | `polyglot-enrich-py` | Python | `valid`, `currency`, `amount` | `usd_amount`, `usd_amount_display` |
| 3 | `polyglot-score-ts` | TypeScript | `usd_amount` | `risk_score`, `flagged` |
| 4 | `polyglot-fee-kt` | Kotlin | `valid`, `region`, `usd_amount` | `fee` |
| 5 | `polyglot-tier-cs` | C# | `flagged`, `risk_score` | `review_tier` |
| 6 | `polyglot-settle-rs` | Rust | `valid`, `review_tier`, `usd_amount`, `fee` | `settlement`, `Ledger` |

Expected output for the six-row fixture. Every value except `settlement` is
produced by a component containing no Rust:

| id | region | currency | amount | valid | usd_amount | usd_amount_display | risk_score | flagged | fee | review_tier | settlement |
|----|--------|----------|--------|-------|------------|--------------------|------------|---------|-----|-------------|------------|
| 1 | emea | EUR | 100 | true | ≈110.0 | 110.00 USD | ≈0.0022 | false | ≈1.32 | 0 | SETTLED |
| 2 | emea | GBP | -5 | false | 0.0 |  | 0.0 | false | 0.0 | 0 | REJECTED |
| 3 | apac | JPY | 1000000 | true | ≈6800.0 | 6800.00 USD | ≈0.136 | false | ≈54.4 | 0 | SETTLED |
| 4 | amer | USD | 60000 | true | 60000.0 | 60000.00 USD | 1.2 | true | 600.0 | 2 | HOLD |
| 5 | emea | EUR | 0 | false | 0.0 |  | 0.0 | false | 0.0 | 0 | REJECTED |
| 6 | apac | USD | 20000 | true | 20000.0 | 20000.00 USD | 0.4 | false | 160.0 | 1 | REVIEW |

## Build and run

Every command here runs the same on Linux, macOS and Windows (PowerShell).

1. Build all six components into `examples/polyglot/build/`, then
   `wasm-tools validate` each:

```text
cargo xtask polyglot
```

With one toolchain, build a single stage. No `emit` step runs, because each
stage derives its own schema in its own language:

```text
cargo xtask polyglot --only=rust
cargo xtask polyglot --only=go,ts
cargo xtask polyglot --only=kotlin,csharp
```

2. Drive the chain: six `describe()` blocks, then two batches and the ledger.
   `tracing` makes the processor host-io metrics visible:

```text
cargo run -p saci-service --features wasm,tracing --example polyglot_orders
```

3. Run the same assertions as an automated regression test. It soft-skips when
   `build/` is absent:

```text
cargo test -p saci-service --features wasm --test polyglot_chain -- --nocapture
```

Each missing tool has its own exit code: 3 wasm-tools, 4 Go, 5
componentize-go, 6 componentize-py, 7 Node or npm, 9 Gradle, 10 wit-bindgen,
11 dotnet, 12 curl. Code 8 means a stage built without producing an artifact.
The Rust stage needs no check: `cargo build --target wasm32-wasip2` links a
component itself.

## Code generation

The `polyglot_schema_emit` example emits the schema constants into the
gitignored `examples/polyglot/generated/`, which also holds the WASI adapter
`cargo xtask polyglot` caches there. The Quick Start and the native-plugin
builds consume those constants; no polyglot stage does. Regenerate them with:

```text
cargo run -p saci-service --features wasm --example polyglot_schema_emit -- emit
```

| file | consumed by |
|------|-------------|
| `order_schema.ipc` | reference copy of the schema-only IPC stream |
| `order_fingerprint.txt` | `cargo xtask quickstart` and `cargo xtask plugins`, which read it to log the fingerprint every stage carries (`f6405a7b`, the 12-field `Order`) |
| `fixture_input.saci` | the five native SDK and codec test suites |
| `fixture_input.json` | ground truth for those tests |
| `schema_gen.go` | rewritten to `package main` and copied to `examples/plugins/settle-go/schema_gen.go` by `cargo xtask plugins` |
| `SchemaGen.cs` | copied to `examples/quickstart/stages/csharp-settle/SchemaGen.cs` by `cargo xtask quickstart` |
| `schema_gen.py`, `schema_gen.ts`, `SchemaGen.kt` | emitted for reference; no build consumes them |

Each stage declares its own `Order` and derives its schema from that
declaration. The driver (`polyglot_orders.rs`) and the integration test load
all six components and assert their reported `schema_fingerprint` values are
equal to each other, so six independently authored schemas must agree
structurally or the load fails.

## Files

| File / directory | What it is |
|------------------|------------|
| `PINS.md` | pinned toolchain versions and the platform caveats |
| `polyglot_orders.rs` | the `saci-service` example that drives the chain |
| `schema_emit.rs` | the example that regenerates `generated/` |
| `stages/go-validate/` | stage 1, Go, writes `valid` |
| `stages/python-enrich/` | stage 2, Python, writes `usd_amount` and `usd_amount_display` |
| `stages/ts-score/` | stage 3, TypeScript, writes `risk_score` and `flagged` |
| `stages/kotlin-fee/` | stage 4, Kotlin, writes `fee` |
| `stages/csharp-tier/` | stage 5, C#, writes `review_tier` |
| `stages/rust-settle/` | stage 6, Rust, writes `settlement` and the ledger |
| `generated/` | gitignored, produced by the `emit` command above |
| `build/` | gitignored, the six `.wasm` components land here |
