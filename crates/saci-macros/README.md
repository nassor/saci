# saci-macros

[![crates.io](https://img.shields.io/crates/v/saci-macros.svg)](https://crates.io/crates/saci-macros)
[![docs.rs](https://img.shields.io/docsrs/saci-macros)](https://docs.rs/saci-macros)
[![License](https://img.shields.io/badge/license-AGPL--3.0--only-blue)](https://github.com/nassor/saci/blob/main/LICENSE)

The derive and attribute macros of the [SACI](https://crates.io/crates/saci) authoring SDK.

Do not depend on this crate directly. `saci` re-exports every macro at its root and in
`saci::prelude`, and the expansions name `::saci::__private::...`, so a processor crate depends
on `saci` alone:

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
| `#[derive(Component)]` | a row struct | `Component::name`, the Arrow schema and one `FieldRef` const per field |
| `#[transform]` | `fn(&mut Row)` or `fn(&mut Row, &sdk::Config)` | a `System` that decodes each row, mutates it in place and re-encodes it |
| `#[fold]` | `fn(&[Row], &mut State)` | a `System` that receives the whole batch once |
| `#[processor]` | `fn() -> Pipeline` | the WIT world, the guest exports and the host-io bridges of a wasm32-wasip3 component |
| `#[plugin]` | `fn() -> Pipeline` | the C ABI exports of a native plugin cdylib |

Each macro leaves the item it annotates as written, so the function stays callable in a unit test.

```rust,ignore
use saci::prelude::*;

#[derive(Component, serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Order {
    pub amount: f64,
    pub usd_amount: f64,
}

#[transform]
pub fn to_usd(row: &mut Order) -> sdk::Result<()> {
    row.usd_amount = row.amount * 1.08;
    Ok(())
}

#[processor]
pub fn build() -> Pipeline {
    Pipeline::new("orders")
        .component::<Order>()
        .system(to_usd_system())
}
```

`cargo build --release --target wasm32-wasip3` turns that crate into a component `saci-service`
loads. The pipeline's name, `orders`, is the descriptor name the host checks.
[Build a Rust processor](https://nassor.github.io/saci/service/processors/build/rust/) walks
through a complete one.

## License

GNU Affero General Public License v3.0 only.
