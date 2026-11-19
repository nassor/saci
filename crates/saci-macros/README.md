# saci-macros

[![crates.io](https://img.shields.io/crates/v/saci-macros.svg)](https://crates.io/crates/saci-macros)
[![docs.rs](https://img.shields.io/docsrs/saci-macros)](https://docs.rs/saci-macros)
[![License](https://img.shields.io/badge/license-AGPL--3.0--only-blue)](https://github.com/nassor/saci/blob/main/LICENSE)

The derive and attribute macros of the [SACI](https://crates.io/crates/saci) processor SDK.

Do not depend on this crate directly. `saci::processor` re-exports every macro, and the expansions
name `::saci::processor::...`, so a processor crate depends on `saci` alone:

```toml
[lib]
crate-type = ["cdylib"]

[dependencies]
saci = { version = "0.1", default-features = false, features = ["processor"] }
serde = { version = "1", features = ["derive"] }

[target.'cfg(target_arch = "wasm32")'.dependencies]
wit-bindgen = { version = "0.63.0", features = ["async"] }
```

## Macros

| Macro | Applies to | Generates |
|---|---|---|
| `#[derive(Component)]` | a row struct | `Component::name` and the Arrow schema |
| `#[transform(...)]` | `fn(&mut Row)` or `fn(&mut Row, &Config)` | a `System` that decodes each row, mutates it in place and re-encodes it |
| `#[fold(...)]` | `fn(&[Row], &mut State)` | a `System` that receives the whole batch once |
| `#[processor(...)]` | `fn() -> Pipeline` | the WIT world, the guest exports and the host-io bridges |

Each macro leaves the item it annotates as written, so the function stays callable in a unit test.

```rust,ignore
use saci::processor::prelude::*;

#[derive(Component, serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Order {
    pub amount: f64,
    pub usd_amount: f64,
}

#[transform(component = Order)]
pub fn to_usd(row: &mut Order) -> Result<()> {
    row.usd_amount = row.amount * 1.08;
    Ok(())
}

#[processor(name = "orders")]
pub fn build() -> Pipeline {
    Pipeline::builder("orders")
        .with::<Order>()
        .with_system(to_usd_system())
        .build()
}
```

`cargo build --release --target wasm32-wasip3` turns that crate into a component `saci-service`
loads. [Build a Rust processor](https://nassor.github.io/saci/service/processors/build/rust/) walks
through a complete one.

## License

GNU Affero General Public License v3.0 only.
