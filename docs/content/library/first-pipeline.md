+++
title = "Build your first pipeline"
description = "A complete native pipeline in nine steps: one component, four systems, one resource, and the stage plan SACI derives instead of you writing it."
template = "page.html"
weight = 1
aliases = ["/native/tutorial/", "/getting-started/"]
+++
# Build your first pipeline

<dl class="page-facts">
<dt>Time</dt>
<dd><strong>15 minutes</strong>, most of it the first <code>cargo build</code></dd>
<dt>You need</dt>
<dd>Rust <strong>1.95</strong> or newer, and a clone of the repository</dd>
<dt>Not needed</dt>
<dd>No database, no broker, no cluster, no WebAssembly toolchain (until step 8)</dd>
</dl>

Every piece of code below is quoted from
`examples/native/first_pipeline.rs`, which CI compiles. The last steps run it
and show the real output.

## 1. Create the project

<div class="note">
<span class="note-label">Before you start</span>

SACI crates are **not published to crates.io**. Every command here assumes you
have cloned the repository and are running from its root.

</div>

```bash,name=Create the project
cargo new order-pipeline
cd order-pipeline
```

Runs the same on Linux, macOS and Windows (PowerShell). Step 1 is done when
`Cargo.toml` and `src/main.rs` exist in `order-pipeline/`.

## 2. Add the dependencies

The engine is `saci`, with its default `engine` feature; the code below imports it as `saci::`.
Inside the clone, it is a path dependency:

```toml,name=The Cargo.toml dependencies
[dependencies]
saci = { path = "../crates/saci" }
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
async-trait = "0.1"
arrow-array = "59.2"
arrow-schema = "59.2"
```

Depend on `saci-service` directly, with its default features, when you also want the host
(wasmtime and the HTTP control plane) in the same binary; `saci`'s `engine` feature carries the
columnar engine alone.

<div class="note">
<span class="note-label">Depending on the engine directly</span>

`saci`'s `engine` feature re-exports `saci-core` behind one path dependency. Depend on
`saci-core` directly, reached through `saci_core::prelude` instead of `saci::prelude`, when a
narrower dependency graph than `saci`'s feature groups is worth the extra path entry. See
[Crates and features](@/library/reference/crates.md) for every crate and feature flag.

</div>

```bash,name=Build the skeleton
cargo build
```

Runs the same on all three platforms. The first build takes most of the 15
minutes while the workspace dependencies compile. Step 2 is done when the build
finishes.

## 3. Describe your data

A `Component` is a plain Rust struct plus the Arrow schema for its fields. SACI
stores one `RecordBatch` per component, so a field is a **column**, not a
per-row lookup.

```rust,name=The Order component and its Arrow schema
#[derive(Serialize, Deserialize, Clone, Debug)]
struct Order {
    id: u64,
    currency: String,
    amount: f64,
    usd_amount: f64,
    express: bool,
}

impl Component for Order {
    fn name() -> &'static str {
        "Order"
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("currency", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("usd_amount", DataType::Float64, false),
            Field::new("express", DataType::Boolean, false),
        ]))
    }
}
```

`FieldRef` constants are how a system names a column without a string literal
at every call site. Declare one per field:

```rust,name=One FieldRef constant per field
impl Order {
    const ID: FieldRef<Order> = FieldRef::new("id");
    const CURRENCY: FieldRef<Order> = FieldRef::new("currency");
    const AMOUNT: FieldRef<Order> = FieldRef::new("amount");
    const USD_AMOUNT: FieldRef<Order> = FieldRef::new("usd_amount");
    const EXPRESS: FieldRef<Order> = FieldRef::new("express");
}
```

[Dataset & Components](@/library/dataset.md) covers registration, appends, soft deletes
and the IPC round trip. Step 3 is done when `cargo check` accepts the file.

## 4. Write the systems

A `System` is one transform plus the `meta()` that declares what it touches.
`SeedOrders` appends the batch every later stage reads.

```rust,name=SeedOrders appends the first batch
struct SeedOrders;

#[async_trait]
impl System for SeedOrders {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("seed").write_component("Order")
    }

    async fn run(&self, data: &mut Dataset) -> Result<(), SaciError> {
        let orders = vec![
            Order {
                id: 1,
                currency: "EUR".into(),
                amount: 120.00,
                usd_amount: 0.0,
                express: false,
            },
            Order {
                id: 2,
                currency: "USD".into(),
                amount: 4300.00,
                usd_amount: 0.0,
                express: false,
            },
            Order {
                id: 3,
                currency: "GBP".into(),
                amount: 75.50,
                usd_amount: 0.0,
                express: false,
            },
            Order {
                id: 4,
                currency: "JPY".into(),
                amount: 900000.00,
                usd_amount: 0.0,
                express: false,
            },
            Order {
                id: 5,
                currency: "EUR".into(),
                amount: 12.00,
                usd_amount: 0.0,
                express: false,
            },
        ];

        let n = orders.len();
        data.append::<Order>(&orders)?;
        println!("seeded {n} orders");
        Ok(())
    }
}
```

`write_component("Order")` means "writes every field of `Order`". That is the
strongest declaration there is, so nothing can share a stage with it, and this
system lands alone in stage 0.

Two more transforms. `ConvertCurrency` writes
`usd_amount` and `FlagExpress` writes `express`. The writes are disjoint, so SACI
puts both in **one stage**, which means nothing orders one before the other. You
never wrote a stage
list. Both implement `ParallelSystem`, which takes `&Dataset` and returns the
columns it produced as a `WriteSet` rather than mutating in place. Five rows is far
under the 100_000 the executor wants before it spends a blocking task per system, so
this run executes them one after another; the stage is what makes either order correct.

```rust,name=ConvertCurrency writes usd_amount
struct ConvertCurrency;

#[async_trait]
impl ParallelSystem for ConvertCurrency {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("convert")
            .reads(Order::AMOUNT)
            .reads(Order::CURRENCY)
            .writes(Order::USD_AMOUNT)
            .read_resource::<FxRates>()
    }

    async fn run(&self, data: &Dataset) -> Result<WriteSet, SaciError> {
        let rates = data
            .get_resource::<FxRates>()
            .ok_or_else(|| SaciError::generic("FxRates resource not found"))?;

        let orders = data.view::<Order>()?;
        let amount = orders.f64(Order::AMOUNT)?;
        let currency = orders.str(Order::CURRENCY)?;
        let values: Vec<f64> = (0..orders.len())
            .map(|i| amount.value(i) * rates.rate_for(currency.value(i)))
            .collect();

        Ok(WriteSet::new().put("Order", "usd_amount", Arc::new(Float64Array::from(values))))
    }
}
```

```rust,name=FlagExpress writes express
struct FlagExpress;

#[async_trait]
impl ParallelSystem for FlagExpress {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("express")
            .reads(Order::AMOUNT)
            .writes(Order::EXPRESS)
    }

    async fn run(&self, data: &Dataset) -> Result<WriteSet, SaciError> {
        let orders = data.view::<Order>()?;
        let amount = orders.f64(Order::AMOUNT)?;
        let flags: Vec<bool> = (0..orders.len())
            .map(|i| amount.value(i) > 1000.0)
            .collect();

        Ok(WriteSet::new().put("Order", "express", Arc::new(BooleanArray::from(flags))))
    }
}
```

Both read `amount`, and two reads never conflict. Add a third system writing
`amount` and the plan re-derives itself with no change to these two.
[Systems](@/library/systems.md) has the full conflict table.

`ConvertCurrency` reads a `FxRates` resource. A `Resource` is a Rust singleton
on the `Dataset`, keyed by type; use one for configuration and lookup tables.
Resources are **not** serialised by `write_ipc`, so they never cross an Arrow
IPC boundary.

```rust,name=The FxRates resource
/// USD-base exchange rates, held as a resource rather than a column.
struct FxRates {
    eur: f64,
    gbp: f64,
    jpy: f64,
}

impl FxRates {
    fn rate_for(&self, currency: &str) -> f64 {
        match currency {
            "EUR" => self.eur,
            "GBP" => self.gbp,
            "JPY" => self.jpy,
            // "USD" is the reporting currency, and an unknown code converts
            // one to one rather than collapsing the row to zero.
            _ => 1.0,
        }
    }
}
```

A transform that does not need its own type can be a closure. `system_fn` pairs
one `SystemMeta` with one `Fn(&mut Dataset)`. This summary system writes a
`Summary` resource for `main` to read:

```rust,name=The summary system built with system_fn
fn make_summary_system() -> impl System {
    system_fn(
        SystemMeta::new("summary")
            .reads(Order::ID)
            .reads(Order::CURRENCY)
            .reads(Order::AMOUNT)
            .reads(Order::USD_AMOUNT)
            .reads(Order::EXPRESS)
            .write_resource::<Summary>(),
        |data| {
            let rows;
            let mut express = 0usize;
            let mut total_usd = 0.0f64;

            {
                let orders = data.view::<Order>()?;
                rows = orders.len();

                let id = orders.u64(Order::ID)?;
                let currency = orders.str(Order::CURRENCY)?;
                let amount = orders.f64(Order::AMOUNT)?;
                let usd = orders.f64(Order::USD_AMOUNT)?;
                let is_express = orders.bool(Order::EXPRESS)?;

                for i in 0..rows {
                    if is_express.value(i) {
                        express += 1;
                    }
                    total_usd += usd.value(i);
                    println!(
                        "order {}  {:>10.2} {} -> {:>10.2} USD  express={}",
                        id.value(i),
                        amount.value(i),
                        currency.value(i),
                        usd.value(i),
                        is_express.value(i)
                    );
                }
            }

            data.insert_resource(Summary {
                rows,
                express,
                total_usd,
            });

            Ok(())
        },
    )
}
```

The inner block matters: the column view borrows the dataset, so it has to drop
before `insert_resource` takes `&mut`. Step 4 is done when `cargo check`
accepts the systems.

## 5. Assemble the pipeline

Register the component, install the resource, add the systems, run.

```rust,name=Assemble the pipeline and run it
#[tokio::main]
async fn main() -> Result<(), SaciError> {
    let mut pipeline = Pipeline::builder("first-pipeline")
        .with::<Order>()
        .with_resource(FxRates {
            eur: 1.08,
            gbp: 1.27,
            jpy: 0.0067,
        })
        .with_system(SeedOrders)
        .with_parallel_system(ConvertCurrency)
        .with_parallel_system(FlagExpress)
        .with_system(make_summary_system())
        .build();

    pipeline.run().await?;

    println!("stages: {:?}", pipeline.stages().unwrap_or_default());

    let summary = pipeline
        .data()
        .get_resource::<Summary>()
        .ok_or_else(|| SaciError::generic("Summary resource missing"))?;
    println!(
        "{} orders, {} express, {:.2} USD total",
        summary.rows, summary.express, summary.total_usd
    );

    Ok(())
}
```

Step 5 is done when `cargo check` accepts `main`.

## 6. Run it

```bash,name=Run the pipeline
cargo run
```

Runs the same on all three platforms. Expected output:

```text,name=Expected console output
seeded 5 orders
order 1      120.00 EUR ->     129.60 USD  express=false
order 2     4300.00 USD ->    4300.00 USD  express=true
order 3       75.50 GBP ->      95.89 USD  express=false
order 4   900000.00 JPY ->    6030.00 USD  express=true
order 5       12.00 EUR ->      12.96 USD  express=false
stages: [[0], [1, 2], [3]]
5 orders, 2 express, 10568.44 USD total
```

`pipeline.stages()` returns `Vec<Vec<usize>>`: one inner vector per stage,
holding system indices in **registration order**. The line above reads
`[[0], [1, 2], [3]]`, which is:

- Stage 0, `SeedOrders` alone, because `write_component` collides with
  everything.
- Stage 1, `ConvertCurrency` and `FlagExpress` together, because their writes
  are disjoint. Five rows runs them in sequence, and a batch of 100_000 or more
  would run them at once.
- Stage 2, the summary closure, because it reads both columns stage 1 wrote.

`stages()` returns `None` until the first `run()` builds the plan.
[Pipeline](@/library/pipeline.md) covers the conflict graph, the topological sort and
per-system retry. Step 6 is done when the numbers above match yours.

## 7. Make it an ETL

The repository's `scheduler_etl` example is the same shape one size up: four
systems over a `Transaction` component, with the validation and enrichment
writes sharing a stage.

```rust,name=The stage 1 pair from scheduler_etl
// Marks each row valid when its amount is positive. Writes only `valid`, which
// is disjoint from EnrichSystem's `usd_amount`, so both land in stage 1.
struct ValidateSystem;

#[async_trait]
impl System for ValidateSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("validate")
            .reads(Transaction::AMOUNT) // decides validity
            .writes(Transaction::VALID) // the only field it touches
    }

    async fn run(&self, pipeline: &mut Dataset) -> Result<(), SaciError> {
        // reads `amount`, builds the `valid` BooleanArray, and swaps it into
        // the component's RecordBatch with `replace_batch`
        ...
    }
}
```

`EnrichSystem` writes only `usd_amount` and reads `amount` plus `currency` and
the `FxRates` resource, so the field-level DAG puts it in the same stage as
`ValidateSystem`:

```text
Stage 0:  [IngestSystem]                  writes every Transaction field
Stage 1:  [ValidateSystem, EnrichSystem]  write "valid" and "usd_amount"
Stage 2:  [ReportSystem]                  reads both
```

The full file, quoted from `examples/native/scheduler_etl.rs`, is the same
primitives you already used, plus `Dataset::replace_batch` for the
column swaps. Run the shipped example to see it:

```bash,name=Run the ETL example
cargo run -p saci-service --example scheduler_etl
```

Runs the same on all three platforms. The visible result is the transaction
report and the derived stage layout:

```text,name=Expected ETL output
Starting ETL pipeline...
[ingest]    loaded 9 transactions
[validate]  7 valid, 2 rejected
[enrich]    converted 9 rows to USD

╔═══════════════════════════════════════════════════════╗
║       ARROW ETL PIPELINE — TRANSACTION REPORT        ║
╠═══════════════════════════════════════════════════════╣
║  #1001      1500.00 USD                                ║
║  #1002      2300.50 EUR →    2484.54 USD           ║
║  #1003       750.00 GBP →     952.50 USD           ║
║  #1004       -50.00 USD  REJECTED                     ║
║  #1005      5000.00 JPY →      33.50 USD           ║
║  #1006       320.75 EUR →     346.41 USD           ║
║  #1007      1200.00 GBP →    1524.00 USD           ║
║  #1008         0.00 USD  REJECTED                     ║
║  #1009       680.00 CAD →     503.20 USD           ║
╠═══════════════════════════════════════════════════════╣
║  Total rows:      9                                  ║
║  Valid:           7                                  ║
║  Rejected:        2                                  ║
║  Total USD:         7344.15                         ║
║  Average USD:       1049.16                         ║
╚═══════════════════════════════════════════════════════╝

Stage layout (field-level DAG):
  Stage 0: [0]
  Stage 1: [1, 2]
  Stage 2: [3]
  (ValidateSystem and EnrichSystem share stage 1
   because they write disjoint fields of Transaction)

Pipeline complete: 7/9 valid, 2 rejected, $7344.15 total USD
```

Step 7 is done when the report prints and the two stage-1 systems share a
stage without any stage list in your code.

## 8. Ship the same pipeline as a component

Make a second crate for the component, at the repository root next to
`order-pipeline/` (`saci`'s `processor` feature pins `saci-core` to its own
`processor` feature, which swaps the rayon stages for sequential execution):

```bash,name=Create the processor crate
cd ..
cargo new order-pipeline-processor --lib
cd order-pipeline-processor
```

Runs the same on all three platforms.

```toml,name=The processor crate manifest
[lib]
crate-type = ["cdylib"]

[dependencies]
saci = { path = "../crates/saci", default-features = false, features = ["processor"] }
serde = { version = "1", features = ["derive"] }

# The bindings generator, wasm32-only like the `bindings` module in lib.rs.
[target.'cfg(target_arch = "wasm32")'.dependencies]
wit-bindgen = "0.62.0"
```

```rust,name=Export the build function
use saci::prelude::*;

// The WIT bindings, generated from the same package the host compiles against.
#[cfg(target_arch = "wasm32")]
#[allow(warnings)]
mod bindings {
    wit_bindgen::generate!({
        path: "../crates/saci-processor/wit",
        world: "saci-pipeline",
        generate_all,
    });
}

fn build() -> Pipeline {
    let mut pipeline = Pipeline::new("order-pipeline");
    pipeline.data.register_component::<Order>().expect("register Order");
    pipeline.add_system(SeedOrders);
    pipeline.add_system(ConvertCurrency);
    pipeline.add_system(FlagExpress);
    pipeline.add_system(make_summary_system());
    pipeline
}

#[cfg(target_arch = "wasm32")]
saci::export_pipeline!(build);
```

`export_pipeline!` writes the `describe` and `run-batch` exports plus the
`saci_config_get` and `saci_config_parse` accessors, all referencing
`crate::bindings`. That is why the bindings module and the wasm32-only
`wit-bindgen` dependency live in this crate.

The `Order` component and the four systems from steps 3 to 5 move into this
crate. Their `arrow_schema::` and `arrow_array::` imports become
`saci::arrow_schema::` and `saci::arrow_array::`, because `saci` re-exports both
crates directly rather than adding them as dependencies here.

The two `ParallelSystem` impls are re-declared as plain `System` impls, because
the processor build has no `runtime` feature and refuses a `ParallelSystem` at
run time. Their column math is unchanged: compute the arrays, then land them
with `data.apply_write_set` instead of returning a `WriteSet`. Build it for the
wasm target, from inside the crate:

```bash,name=Build the component
rustup target add wasm32-wasip2
cargo build --release --target wasm32-wasip2
```

Runs the same on all three platforms. The artifact takes the crate name with
hyphens turned to underscores, so step 8 is done when
`target/wasm32-wasip2/release/order_pipeline_processor.wasm` exists.
`saci-service` loads that file through a `wasm` node in a KDL config, with
sources and sinks declared around it; `examples/configs/standalone_wasm.kdl`
is the shape. [The Rust processor page](@/service/processors/build/rust.md) walks the
full authoring flow, and [Workflows and links](@/service/config/workflows.md) is
the config block that wires the node up.

## 9. Where to go next

- [Sources & Sinks](@/library/io.md): read and write Parquet, CSV and NDJSON instead of
  hardcoding a seed system.
- [Scheduler](@/library/scheduler.md): several pipelines in one process, with dependency
  edges between them.
- [Distributed processing](@/library/distributed.md): the same pipeline against claimed row
  ranges across nodes, with checkpoints.
- [Native plugins](@/service/plugins/rust.md): the same pipeline as a shared library
  the service loads at runtime, without the sandbox.
