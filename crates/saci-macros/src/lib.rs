//! Derive and attribute macros for SACI.
//!
//! Re-exported from `saci` (and `saci::prelude`); a crate depends on `saci` and
//! never on this crate directly. Five macros:
//!
//! | Macro                  | Applies to                                    | Generates                                              |
//! |------------------------|-----------------------------------------------|--------------------------------------------------------|
//! | `#[derive(Component)]` | a row struct                                  | `Component::name`, `Component::schema`, field consts   |
//! | `#[transform]`         | `fn(&mut Row)` / `fn(&mut Row, &sdk::Config)` | a `System` that decodes, mutates in place, re-encodes  |
//! | `#[fold]`              | `fn(&[Row], &mut State)`                      | a `System` that hands the whole batch over once        |
//! | `#[processor]`         | `fn() -> Pipeline`                            | the WIT world and the WebAssembly guest exports        |
//! | `#[plugin]`            | `fn() -> Pipeline`                            | the native plugin C ABI exports                        |
//!
//! Every macro leaves the item it is applied to exactly as written, so the
//! author's function stays callable and unit-testable outside any pipeline.
//!
//! # Paths in the expansions
//!
//! Generated code names `::saci::__private::...`, so `saci` must be a direct
//! dependency of the crate using these macros. `#[processor]` also names
//! `crate::bindings`, the `wit_bindgen::generate!` module it emits: WIT
//! bindings are caller-side, so `#[processor]` belongs on a `fn` at the
//! processor crate's root.

#![deny(missing_docs)]

use proc_macro::TokenStream;
use syn::{DeriveInput, ItemFn, parse_macro_input};

mod component;
mod plugin;
mod processor;
mod system;
mod util;

/// The `saci:pipeline@0.4.0` world text, vendored from `saci`.
///
/// `#[processor]` splices it into `wit_bindgen::generate!({ inline: ... })`,
/// whose `path:` alternative would resolve against each processor crate's
/// own manifest directory and force every one of them to hand-write a
/// `../../..` walk. Read from this crate's own `wit/` directory, not
/// `saci`'s: `cargo package`/`publish` never includes files that
/// live outside the package being packaged, so a `../../saci`
/// reference would silently drop out of a published `saci-macros` tarball
/// and fail to compile from it. `wit/README.md` and
/// `tests/wit_vendored.rs` keep the two copies from drifting.
const PIPELINE_WIT: &str = include_str!("../wit/pipeline.wit");

/// Implement `Component` for a row struct.
///
/// Fills the two required methods:
///
/// - `name()` returns the struct identifier, or the `#[saci(name = "...")]`
///   override.
/// - `schema()` traces the Arrow schema from the type with `serde_arrow`, once
///   per process, pinned to the 32-bit offset forms (`Utf8`, `List`, `Binary`)
///   that every non-Rust SACI codec assumes. Field order is declaration order,
///   which is what the schema fingerprint hashes.
///
/// It also declares one `FieldRef` constant per field, named after the field
/// in upper case (`usd_amount` → `Order::USD_AMOUNT`) and pointing at the
/// column serde writes for it, `#[serde(rename)]` and
/// `#[serde(rename_all)]` included. A skipped or flattened field has none.
/// `#[saci(fields = false)]` declares none at all.
///
/// # Requires `Serialize` and `Deserialize`
///
/// Derive them alongside:
///
/// ```ignore
/// #[derive(Component, serde::Serialize, serde::Deserialize)]
/// pub struct Order {
///     pub id: i64,
///     pub region: String,
/// }
/// ```
///
/// `schema()` traces the type's `Deserialize` impl, and `Component`'s default
/// `to_record_batch` / `from_record_batch` are `serde_arrow` calls. A derive
/// macro emits impls, not derives, so it cannot add them for you; `serde` also
/// has to be a direct dependency of the crate, because serde's own expansion
/// names the literal `serde` crate at the call site.
///
/// # Renaming a column
///
/// Use `#[serde(rename = "...")]`. `#[saci(...)]` on a field is rejected: the
/// schema is traced from the `Deserialize` impl, so a serde rename moves the
/// schema field name and the encoder together, while a schema-only rename
/// would desynchronise them and break every encode and decode.
#[proc_macro_derive(Component, attributes(saci))]
pub fn derive_component(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    component::expand(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Turn a row function into a `System` that rewrites one component in place.
///
/// ```ignore
/// #[transform]
/// pub fn settle(row: &mut Order) -> sdk::Result<()> {
///     row.settlement = if row.valid { "SETTLED" } else { "REJECTED" }.to_string();
///     Ok(())
/// }
/// ```
///
/// Adds `settle_system() -> impl System` next to the untouched `settle`. The
/// component is the type behind the first parameter, which must be written
/// `&mut T`. The generated system decodes the component's rows, calls the
/// function once per row, and writes the batch back with
/// `Dataset::replace_batch`.
///
/// A second parameter opts into host config, built once per batch:
///
/// ```ignore
/// #[transform]
/// pub fn settle(row: &mut Order, config: &sdk::Config) -> sdk::Result<()> {
///     let floor = config.get("min_amount", 0.0)?;
///     # let _ = floor;
///     Ok(())
/// }
/// ```
///
/// # Declared access
///
/// The system declares a whole-component read *and* write of the component.
/// Over-declaration is safe by construction: the DAG expands a whole-component
/// declaration into every field of that component, which can only add edges,
/// and edges only cost parallelism.
///
/// # Errors
///
/// An error out of the function fails the batch as
/// `SaciError::SystemExecution`, which is retryable.
#[proc_macro_attribute]
pub fn transform(attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);
    util::reject_args(attr.into(), "transform")
        .and_then(|()| system::expand_transform(func))
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Turn a batch function into a `System` that reads a component and writes
/// cross-batch state.
///
/// ```ignore
/// #[fold]
/// pub fn ledger(rows: &[Order], state: &mut Ledger) -> sdk::Result<()> {
///     state.settled_count += rows.len() as i64;
///     Ok(())
/// }
/// ```
///
/// Adds `ledger_system() -> impl System` next to the untouched `ledger`. The
/// component is the element type of the first parameter (`&[T]`), the state
/// the type behind the second (`&mut S`). The generated system decodes every
/// row, fetches the `sdk::State<S>` resource the pipeline's `.state::<S>()`
/// put on the dataset (inserting `S::default()` on a cold start), and calls
/// the function once with the whole slice.
///
/// # Errors
///
/// An error out of the function fails the batch as
/// `SaciError::SystemExecution`, which is retryable.
#[proc_macro_attribute]
pub fn fold(attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);
    util::reject_args(attr.into(), "fold")
        .and_then(|()| system::expand_fold(func))
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Export a pipeline as a `saci:pipeline@0.4.0` WebAssembly component.
///
/// ```ignore
/// #[processor]
/// pub fn build() -> Pipeline {
///     Pipeline::new("settle")
///         .component::<Order>()
///         .state::<Ledger>()
///         .system(settle_system())
///         .system(ledger_system())
/// }
/// ```
///
/// Emits, all under `#[cfg(target_arch = "wasm32")]` so the author's source
/// carries no target gates: `mod bindings`, a `wit_bindgen::generate!` over
/// the `saci-pipeline` world, the `Guest` impl covering `describe` and
/// `run-batch`, the `sdk::host` backend over the `host-io` imports, and the
/// `crate::bindings::export!` handshake. `fn build()` itself is re-emitted
/// unchanged and compiles for every target.
///
/// The pipeline's name is the `pipeline-descriptor.name` the host gates config
/// and checkpoint compatibility on.
///
/// # Requirements
///
/// - Apply it to a `fn` at the crate root: the expansion names
///   `crate::bindings`.
/// - The crate needs `wit-bindgen` with the `async` feature enabled as a
///   `cfg(target_arch = "wasm32")` dependency, and is built with
///   `cargo build --target wasm32-wasip3`.
/// - The crate must not carry a `wit/` directory of its own.
///   `wit_bindgen::generate!` parses `<crate>/wit` when it exists even
///   alongside `inline:`, and a second copy of `saci:pipeline` there would
///   collide with the one this macro splices in.
#[proc_macro_attribute]
pub fn processor(attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);
    util::reject_args(attr.into(), "processor")
        .and_then(|()| processor::expand(func))
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Export a pipeline as a native plugin: a `cdylib` the host `dlopen`s.
///
/// ```ignore
/// #[plugin]
/// pub fn build() -> Pipeline {
///     Pipeline::new("settle").component::<Order>().system(settle_system())
/// }
/// ```
///
/// Emits the two exported symbols the C ABI requires, `saci_abi_version` and
/// `saci_plugin_v1`, and the four vtable thunks they install. Every call into
/// the pipeline runs inside `catch_unwind`, so a panic becomes a `PERMANENT`
/// status instead of unwinding across the boundary. The crate must set
/// `crate-type = ["cdylib"]`.
#[proc_macro_attribute]
pub fn plugin(attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);
    util::reject_args(attr.into(), "plugin")
        .and_then(|()| plugin::expand(func))
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
