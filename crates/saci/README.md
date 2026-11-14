# saci

[![crates.io](https://img.shields.io/crates/v/saci.svg)](https://crates.io/crates/saci)
[![docs.rs](https://img.shields.io/docsrs/saci)](https://docs.rs/saci)
[![License](https://img.shields.io/badge/license-AGPL--3.0--only-blue)](https://github.com/nassor/saci/blob/main/LICENSE)

A columnar batch processing engine on Apache Arrow. Each transform declares the Arrow fields it
reads and writes; SACI builds the dependency graph from those declarations, runs transforms that
cannot conflict in the same stage, and retries what fails.

The crate carries the engine, the connectors, the byte formats, the WebAssembly processor and
native plugin SDKs, and the `saci-service` host. Each part sits behind a feature, so a build
compiles only what it names.

## Install

```toml
[dependencies]
saci = "0.1"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
async-trait = "0.1"
```

The default feature, `engine`, is the engine alone. Rust 1.99 or newer.

## Example

One component and one system. `ToUsd` reads `amount` and writes `usd_amount`; that declaration is
the only scheduling input the pipeline needs.

```rust
use std::sync::Arc;

use async_trait::async_trait;
use saci::SaciError;
use saci::arrow_array::Float64Array;
use saci::arrow_schema::{DataType, Field, Schema};
use saci::component::Component;
use saci::dataset::Dataset;
use saci::pipeline::Pipeline;
use saci::system::{FieldRef, ParallelSystem, SystemMeta, WriteSet};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Order {
    amount: f64,
    usd_amount: f64,
}

impl Component for Order {
    fn name() -> &'static str {
        "Order"
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("amount", DataType::Float64, false),
            Field::new("usd_amount", DataType::Float64, false),
        ]))
    }
}

impl Order {
    const AMOUNT: FieldRef<Order> = FieldRef::new("amount");
    const USD_AMOUNT: FieldRef<Order> = FieldRef::new("usd_amount");
}

struct ToUsd;

#[async_trait]
impl ParallelSystem for ToUsd {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("to_usd")
            .reads(Order::AMOUNT)
            .writes(Order::USD_AMOUNT)
    }

    async fn run(&self, data: &Dataset) -> Result<WriteSet, SaciError> {
        let orders = data.view::<Order>()?;
        let amount = orders.f64(Order::AMOUNT)?;
        let usd: Vec<f64> = (0..orders.len()).map(|i| amount.value(i) * 1.08).collect();
        Ok(WriteSet::new().put("Order", "usd_amount", Arc::new(Float64Array::from(usd))))
    }
}

#[tokio::main]
async fn main() -> Result<(), SaciError> {
    let mut pipeline = Pipeline::builder("orders")
        .with::<Order>()
        .with_parallel_system(ToUsd)
        .build();
    pipeline.data_mut().append::<Order>(&[
        Order { amount: 120.0, usd_amount: 0.0 },
        Order { amount: 12.0, usd_amount: 0.0 },
    ])?;
    pipeline.run().await?;

    let usd = pipeline.data().column::<Order>("usd_amount");
    println!("{usd:?}");
    Ok(())
}
```

The package carries nine runnable examples in `examples/`. From a clone of the repository:

```bash
cargo run -p saci --example first_pipeline
```

## Features

| Feature | What it adds |
|---|---|
| `engine` (default) | `Dataset`, `Component`, `System`, `Pipeline`, `Scheduler`, the `Source` and `Sink` traits |
| `windows` | Tumbling, sliding and session windows with watermarks |
| `connector-<name>` | One transport in `saci::connector`; `connectors` enables the seven that need no external service |
| `transformer-<format>` | One byte format in `saci::transformer`; `transformers` enables all five |
| `processor` | The wasm32-wasip2 processor SDK and the `saci:pipeline@0.3.0` WIT world |
| `plugin` | The native plugin SDK and its C ABI |
| `service`, `wasm-host`, `plugin-host` | The host: KDL config, runners, HTTP control plane, the two processor hosts |
| `server` | The `saci-service` binary with the self-contained connectors and every format |

A processor crate depends on `saci = { version = "0.1", default-features = false, features =
["processor"] }`, since tokio cannot target wasm32-wasip2.
[Crates and features](https://nassor.github.io/saci/library/reference/crates/) lists every feature.

## The service binary

```bash
cargo install saci --features server
saci-service --version
```

`saci-service` loads WebAssembly processor components named in a KDL config and drives Arrow
batches from sources through them to sinks.
[Your first pipeline](https://nassor.github.io/saci/service/first-pipeline/) runs one end to end.

## Links

- [Documentation site](https://nassor.github.io/saci/)
- [API reference](https://docs.rs/saci)
- [Repository](https://github.com/nassor/saci)
- [Changelog](https://github.com/nassor/saci/blob/main/CHANGELOG.md)

## License

GNU Affero General Public License v3.0 only.
