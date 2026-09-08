//! Derive and attribute macros for the SACI processor SDK.
//!
//! Re-exported from `saci-processor`; a processor crate depends on that and
//! never on this crate directly. Four macros cover a whole processor:
//!
//! | Macro                    | Applies to                                        | Generates                                              |
//! |--------------------------|---------------------------------------------------|--------------------------------------------------------|
//! | `#[derive(Component)]`   | a row struct                                      | `Component::name` and `Component::schema`              |
//! | `#[transform(...)]`      | `fn(&mut Row)` / `fn(&mut Row, &Config)`          | a `System` that decodes, mutates in place, re-encodes  |
//! | `#[fold(...)]`           | `fn(&[Row], &mut State)`                          | a `System` that hands the whole batch over once        |
//! | `#[processor(...)]`      | `fn() -> Pipeline`                                | the WIT world, the guest exports, the host-io shims    |
//!
//! Every macro leaves the item it is applied to exactly as written, so the
//! author's function stays callable and unit-testable outside any pipeline.
//!
//! # Paths in the expansions
//!
//! Generated code names `::saci_processor::...`, so `saci-processor` must be a
//! direct dependency of the crate using these macros. Two references are
//! deliberately unqualified instead:
//!
//! - `crate::bindings`, the `wit_bindgen::generate!` module `#[processor]`
//!   emits. WIT bindings are caller-side, so this must resolve to the processor
//!   crate, not to `saci-processor`.
//! - `crate::saci_config_get`, which `#[processor]` emits for the same reason
//!   and a two-parameter `#[transform]` reads.
//!
//! Both consequences are the same: `#[processor]` belongs on a `fn` at the
//! processor crate's root.

#![deny(missing_docs)]

use proc_macro::TokenStream;
use syn::{DeriveInput, ItemFn, parse_macro_input};

mod component;
mod processor;
mod system;
mod util;

/// The `saci:pipeline@0.3.0` world text, vendored from `saci-processor`.
///
/// `#[processor]` splices it into `wit_bindgen::generate!({ inline: ... })`,
/// whose `path:` alternative would resolve against each processor crate's
/// own manifest directory and force every one of them to hand-write a
/// `../../..` walk. Read from this crate's own `wit/` directory, not
/// `saci-processor`'s: `cargo package`/`publish` never includes files that
/// live outside the package being packaged, so a `../../saci-processor`
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
/// - `schema()` traces the Arrow schema from the type with `serde_arrow`,
///   pinned to the 32-bit offset forms (`Utf8`, `List`, `Binary`) that every
///   non-Rust SACI codec assumes. Field order is declaration order, which is
///   what the schema fingerprint hashes.
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
/// has to be a direct dependency of the processor crate, because serde's own
/// expansion names the literal `serde` crate at the call site.
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
/// #[transform(component = Order)]
/// pub fn settle(row: &mut Order) -> saci_processor::Result<()> {
///     row.settlement = if row.valid { "SETTLED" } else { "REJECTED" }.to_string();
///     Ok(())
/// }
/// ```
///
/// Adds `settle_system() -> impl System` next to the untouched `settle`. The
/// generated system decodes the component's rows, calls the function once per
/// row, and writes the batch back with `Dataset::replace_batch`.
///
/// A second parameter opts into host config; the parameter count alone selects
/// the form, so the type may be spelled any way that resolves:
///
/// ```ignore
/// #[transform(component = Order)]
/// pub fn settle(row: &mut Order, config: &saci_processor::Config) -> saci_processor::Result<()> {
///     let floor = config.get("min_amount", 0.0)?;
///     # let _ = floor;
///     Ok(())
/// }
/// ```
///
/// The `Config` is built once per batch, not per row.
///
/// # Declared access
///
/// The system declares a whole-component read *and* write of `component`.
/// Over-declaration is safe by construction: the DAG expands a whole-component
/// declaration into every field of that component, which can only add edges,
/// and edges only cost parallelism.
///
/// # Scope
///
/// Reads and writes name the same component, because the function mutates a row
/// in place. A batch-level operation, or one that writes state rather than
/// columns, is a `#[fold]`.
///
/// # Errors
///
/// An error out of the function fails the batch as
/// `SaciError::SystemExecution`, which the host classifies as retryable.
#[proc_macro_attribute]
pub fn transform(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as system::TransformArgs);
    let func = parse_macro_input!(item as ItemFn);
    system::expand_transform(args, func)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Turn a batch function into a `System` that reads a component and writes
/// cross-batch state.
///
/// ```ignore
/// #[fold(reads = Order, state = Ledger)]
/// pub fn ledger(rows: &[Order], state: &mut Ledger) -> saci_processor::Result<()> {
///     state.settled_count += rows.len() as i64;
///     Ok(())
/// }
/// ```
///
/// Adds `ledger_system() -> impl System` next to the untouched `ledger`. The
/// generated system decodes every row of `reads`, fetches the
/// `ProcessorState<State>` resource the `#[processor]` `state = ...` form put
/// on the dataset (inserting `State::default()` on a cold start), and calls the
/// function once with the whole slice.
///
/// The declared access is a whole-component read of `reads` plus a resource
/// write of `ProcessorState<State>`, so a fold is scheduled after any
/// `#[transform]` writing that component.
///
/// `State` must be a `Component` implementing `Default`, `Serialize` and
/// `Deserialize`. It is *state*, not a batch component: never register it on
/// the dataset. `Dataset`'s IPC format requires every registered component to
/// hold exactly the dataset's row count, while state rows are independent of
/// batch rows.
///
/// # Errors
///
/// An error out of the function fails the batch as
/// `SaciError::SystemExecution`, which the host classifies as retryable.
#[proc_macro_attribute]
pub fn fold(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as system::FoldArgs);
    let func = parse_macro_input!(item as ItemFn);
    system::expand_fold(args, func)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Export a pipeline as a `saci:pipeline@0.3.0` WebAssembly component.
///
/// ```ignore
/// #[processor(name = "polyglot-settle-rs", state = Ledger)]
/// pub fn build() -> Pipeline {
///     Pipeline::builder("polyglot-settle-rs")
///         .with::<Order>()
///         .with_system(settle_system())
///         .with_system(ledger_system())
///         .build()
/// }
/// ```
///
/// Emits, all under `#[cfg(target_arch = "wasm32")]` so the author's source
/// carries no target gates:
///
/// - `mod bindings`, a `wit_bindgen::generate!` over the `saci-pipeline` world
///   text embedded in this crate,
/// - the `Guest` impl covering `describe` and `run-batch`, including state
///   restore/capture, `RouteDecision` read-back, error classification and
///   `RunMetrics`,
/// - the `crate::bindings::export!` handshake.
///
/// Plus four host-io functions with host-target stand-ins, which must be
/// emitted into the processor crate because WIT bindings are caller-side:
/// `saci_config_get`, `saci_config_parse`, `log(target, message)` and
/// `metric(name, value)`.
///
/// `fn build()` itself is re-emitted unchanged and compiles for every target.
///
/// # Arguments
///
/// - `name = "..."` (required) is the `pipeline-descriptor.name` the host gates
///   config and checkpoint compatibility on. It is the processor's external
///   identity; the string passed to `Pipeline::builder` names the DAG in
///   diagnostics and is normally the same.
/// - `state = T` (optional) names the one `Component` whose rows survive across
///   `run-batch` calls; omit it for a stateless processor. `T` must not be
///   registered on the dataset.
///
/// # Requirements
///
/// - Apply it to a `fn` at the crate root: the expansion names
///   `crate::bindings`.
/// - The crate needs `wit-bindgen` as a `cfg(target_arch = "wasm32")`
///   dependency, and is built with `cargo build --target wasm32-wasip2`.
/// - The crate must not carry a `wit/` directory of its own.
///   `wit_bindgen::generate!` parses `<crate>/wit` when it exists even
///   alongside `inline:`, and a second copy of `saci:pipeline` there would
///   collide with the one this macro splices in.
#[proc_macro_attribute]
pub fn processor(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as processor::ProcessorArgs);
    let func = parse_macro_input!(item as ItemFn);
    processor::expand(args, func)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
