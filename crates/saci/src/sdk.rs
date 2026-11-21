//! The authoring SDK: everything a WebAssembly processor or a native plugin
//! names besides the engine.
//!
//! A processor is a `cdylib` crate built for wasm32-wasip3 that depends on
//! `saci` with `default-features = false, features = ["processor"]`; a native
//! plugin is a `cdylib` the host loads with `dlopen`, depending on `saci` with
//! `features = ["plugin"]`. Both are one row struct plus one function per
//! transform, and one function that builds the [`Pipeline`]:
//!
//! ```ignore
//! use saci::prelude::*;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Component, Serialize, Deserialize, Default, Clone)]
//! struct Order { id: i64, amount: f64, valid: bool }
//!
//! #[transform]
//! fn validate(row: &mut Order, config: &sdk::Config) -> sdk::Result<()> {
//!     row.valid = row.amount >= config.get("min_amount", 0.0)?;
//!     Ok(())
//! }
//!
//! #[processor]            // a wasm32-wasip3 component; `#[plugin]` for a native cdylib
//! fn build() -> Pipeline {
//!     Pipeline::new("validate").component::<Order>().system(validate_system())
//! }
//! ```
//!
//! `#[processor]` emits the `saci:pipeline@0.4.0` WIT world and its two
//! exports, `describe` and `run-batch`, all under
//! `#[cfg(target_arch = "wasm32")]`, so the source above carries no target
//! gates and `build()` stays testable on the host. `#[plugin]` emits the two
//! C ABI symbols of [`plugin_abi`](crate::plugin_abi). The pipeline's name is
//! the identity the host reports and gates checkpoint compatibility on.
//!
//! # Choosing between a plugin and a processor
//!
//! A WebAssembly processor is sandboxed, portable, and preemptible: the host
//! bounds it with a wasmtime epoch deadline and a trap cannot take the service
//! down. A native plugin has none of that. It runs in-process with full host
//! privileges, cannot be interrupted, and a memory error in it is a memory
//! error in the host. What it buys is the absence of the sandbox: native
//! threads, native extensions, and no copy through wasm linear memory.
//!
//! # Config, logs and metrics
//!
//! [`host`] reads the `config` values of the processor's or plugin's node in
//! the service config, and sends logs and metrics to the host. A
//! two-parameter `#[transform]` receives a [`Config`], which reads them typed
//! with a default for an absent key.
//!
//! # State across batches
//!
//! Nothing in a processor's linear memory survives a batch boundary, and only
//! the checkpoint travels with a plugin's batch. A pipeline that needs state
//! declares exactly one state component with
//! [`Pipeline::state`](crate::Pipeline::state):
//!
//! ```ignore
//! Pipeline::new("ledger").component::<Order>().state::<Ledger>().system(ledger_system())
//! ```
//!
//! The previous batch's rows arrive as a [`State`]`<Ledger>` resource on the
//! batch dataset; whatever the systems leave in it becomes the checkpoint the
//! host persists and hands back. A `#[fold]` reaches it through
//! [`State::get_or_insert_default`]. State is a resource, not a registered
//! component, because the dataset's Arrow IPC format requires every component
//! to hold exactly the batch's row count while state rows are independent of
//! batch rows; state therefore never appears in the output.
//!
//! # Routing
//!
//! A system that inserts a [`RouteDecision`] into the batch dataset picks the
//! branches the output is delivered to; with none, the host delivers it to
//! every downstream link.
//!
//! # Errors
//!
//! A failed run reaches the host as retryable when
//! [`SaciError::is_retryable`](crate::SaciError::is_retryable) says so
//! (`SystemExecution`, `Store`, `RetryExhausted`) and as permanent otherwise.
//! An [`Error`] out of a `#[transform]` or `#[fold]` function becomes a
//! `SystemExecution`, so a failing row releases the batch for another attempt.
//! Construct errors instead of `.unwrap()` or `panic!()`: a panic becomes a
//! wasm trap or a caught `PERMANENT` plugin status, and the batch is lost.
//!
//! [`Pipeline`]: crate::Pipeline

mod config;
mod error;
pub(crate) mod glue;
pub mod host;
#[cfg(feature = "plugin")]
pub(crate) mod plugin_rt;
pub(crate) mod state;

pub use config::Config;
pub use error::{Error, Result};
pub use state::{RouteDecision, State};

/// The name 0.2 gave [`State`].
#[deprecated(since = "0.3.0", note = "use `sdk::State`")]
pub type ProcessorState<C> = State<C>;
