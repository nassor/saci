//! `saci-plugin`: processor SDK for SACI native plugins.
//!
//! A native plugin is a shared library the host loads with `dlopen` or
//! `LoadLibrary`, exporting the C ABI that `saci-plugin-abi` defines. It runs the
//! same [`Pipeline`] a WebAssembly processor would, against the same Arrow IPC wire
//! format, without a wasm toolchain and without the sandbox.
//!
//! # Authoring a plugin
//!
//! Write a crate with `crate-type = ["cdylib"]` that depends on `saci-plugin`,
//! add a `fn` that builds a [`Pipeline`], and wire it up:
//!
//! ```ignore
//! use saci_plugin::prelude::*;
//!
//! fn build() -> Pipeline {
//!     let mut pipeline = Pipeline::new("my-plugin");
//!     // pipeline.data.register_component::<MyComponent>().unwrap();
//!     // pipeline.add_system(MySystem);
//!     pipeline
//! }
//!
//! saci_plugin::export_plugin!(build);
//! ```
//!
//! `cargo build --release` on that crate produces the shared library. Point
//! a `plugin` node in the service config at it, or load it directly with
//! `saci_service::plugin::NativePluginRuntime::open`.
//!
//! # Choosing between a plugin and a processor
//!
//! A WebAssembly processor is sandboxed, portable, and preemptible: the host bounds
//! it with a wasmtime epoch deadline and a trap cannot take the service down. A
//! native plugin has none of that. It runs in-process with full host
//! privileges, cannot be interrupted, and a memory error in it is a memory
//! error in the host.
//!
//! What it buys is the absence of the sandbox: native threads, native
//! extensions, no componentizer, and no copy through wasm linear memory. Reach
//! for a plugin when the workload needs something WASI 0.2 does not offer, and
//! for a processor otherwise.
//!
//! # Config
//!
//! Values from the `config` node inside the service config's `plugin` node are read
//! through the host's `get_config` callback, which the host answers on every
//! call including `describe`. [`export_plugin!`] emits `saci_config_get` and
//! `saci_config_parse` into the plugin crate.
//!
//! # State across batches
//!
//! The host hands back whatever the plugin returned as its checkpoint, and
//! nothing else survives a batch boundary. A plugin that needs state declares
//! exactly one state component:
//!
//! ```ignore
//! saci_plugin::export_plugin!(build, state = Counter);
//! ```
//!
//! The macro decodes the previous batch's rows into a [`ProcessorState`]`<Counter>`
//! resource on the batch dataset, runs the pipeline, then serialises whatever
//! the systems left there into the checkpoint. State is a resource rather than a
//! registered component because [`Dataset`]'s Arrow IPC format requires every
//! component to hold exactly the dataset's row count, while state rows are
//! independent of batch rows, so state never appears in the plugin's output.
//!
//! Unlike a WebAssembly processor, a plugin's process memory does survive between
//! calls, so a plugin *could* keep state in a global. It should not: the
//! distributed runner may hand consecutive batches of the same partition to
//! different processes, and only the checkpoint travels with the claim.
//!
//! # Error handling in user systems
//!
//! The generated `run_batch` converts a [`SaciError`] out of
//! `Pipeline::run_on_with_stats` into a status the host understands:
//!
//! | `SaciError` variant                                        | status      |
//! |------------------------------------------------------------|-------------|
//! | `RetryExhausted`, `SystemExecution`                        | `RETRYABLE` |
//! | `ComponentNotFound`, `ResourceNotFound`, `EntityNotFound`, |             |
//! | `Configuration`, `Scheduler`, `Store`, `Generic`           | `PERMANENT` |
//!
//! Construct errors explicitly instead of `.unwrap()` or `panic!()` inside
//! `System::run`. `export_plugin!` catches a panic at this boundary and
//! reports `PERMANENT` regardless, so nothing crosses the FFI boundary, but
//! the batch is lost either way and the message is a panic location instead
//! of your own words. Returning a `SaciError::SystemExecution` instead lets
//! the runner release the claim and retry, with a message you chose.

#![deny(missing_docs)]

pub use saci_core::{
    Component, Dataset, Pipeline, PipelineBuilder, RetryMode, Row, RunStats, SaciError, SaciResult,
    SchemaRegistry, System, SystemConfig, SystemMeta, WriteSet, system_fn,
};

// Arrow sub-crates plugin authors need to define `Component` schemas,
// re-surfaced at the workspace-pinned `=59.3.0` version so plugin crates do not
// depend on them directly. `serde` is not re-exported: `#[derive(Serialize)]`
// expansions reference the literal `::serde` path at the call site, so plugin
// authors add `serde` as a direct dep of their own crate.
pub use arrow_array;
pub use arrow_schema;

pub use saci_core::sdk::ProcessorState;
pub use saci_core::sdk::RouteDecision;

/// The windowing primitives (`WindowSpec`, `WatermarkState`,
/// `WindowedSystemBuilder`, ...), behind the `windows` feature.
///
/// A plugin that implements Beam-style windowing over the merged rows it
/// receives keeps its open windows in its checkpoint state and uses these
/// primitives for the window maths; the host tracks the node's watermark
/// separately and reports it as `saci_window_watermark_seconds`.
#[cfg(feature = "windows")]
pub use saci_core::windows;

/// A curated prelude for plugin crates.
///
/// `use saci_plugin::prelude::*;` imports the most common types for building a
/// pipeline: [`Component`], [`Dataset`], [`Pipeline`], [`System`], and the
/// error and metadata types needed to define systems.
pub mod prelude {
    pub use saci_core::prelude::*;
}

#[doc(hidden)]
#[path = "rt.rs"]
pub mod __rt;

/// Logging and metrics through the host.
///
/// The plugin side of the ABI's host callbacks, and the native counterpart of
/// the WIT `host-io` interface a WebAssembly processor imports. A call made outside
/// a boundary call, or against a host that supplied no callback, is dropped
/// rather than failing: observability must never be the reason a batch dies.
pub mod host {
    use crate::__rt;
    use crate::__rt::log_level;

    /// Emit a trace-level line.
    pub fn trace(target: &str, message: &str) {
        __rt::host_log(log_level::TRACE, target, message);
    }

    /// Emit a debug-level line.
    pub fn debug(target: &str, message: &str) {
        __rt::host_log(log_level::DEBUG, target, message);
    }

    /// Emit an info-level line.
    pub fn info(target: &str, message: &str) {
        __rt::host_log(log_level::INFO, target, message);
    }

    /// Emit a warn-level line.
    pub fn warn(target: &str, message: &str) {
        __rt::host_log(log_level::WARN, target, message);
    }

    /// Emit an error-level line.
    pub fn error(target: &str, message: &str) {
        __rt::host_log(log_level::ERROR, target, message);
    }

    /// Record a metric value. The host routes it to its own registry.
    pub fn metric(name: &str, value: f64) {
        __rt::host_metric(name, value);
    }
}

/// Wire a user-authored pipeline to the native plugin C ABI.
///
/// The first argument names a `fn() -> Pipeline` that builds the pipeline. The
/// optional `state = T` argument names the one [`Component`] whose rows survive
/// across batches. The macro generates, in the calling crate:
///
/// - the two exported symbols the ABI requires, `saci_abi_version` and
///   `saci_plugin_v1`,
/// - the four vtable thunks they install,
/// - `saci_config_get` and `saci_config_parse` free functions.
///
/// # Requirements
///
/// - The crate must set `crate-type = ["cdylib"]`, or nothing exports the
///   symbols.
/// - A `state = T` plugin must NOT register `T` in `build()`: the macro installs
///   `ProcessorState<T>` as a resource before the systems run and serializes it
///   into the checkpoint afterwards. Resources do not round-trip through Arrow
///   IPC, so state never leaks into the output.
///
/// # Example
///
/// ```ignore
/// use saci_plugin::prelude::*;
///
/// fn build() -> Pipeline {
///     Pipeline::new("demo")
/// }
///
/// saci_plugin::export_plugin!(build);
/// ```
///
/// See the crate documentation for the error-handling contract.
#[macro_export]
macro_rules! export_plugin {
    ($build_fn:ident) => {
        $crate::export_plugin!(@impl $build_fn, $crate::__rt::NoState);
    };
    ($build_fn:ident, state = $state:ty) => {
        $crate::export_plugin!(@impl $build_fn, $crate::__rt::Stateful<$state>);
    };
    (@impl $build_fn:ident, $spec:ty) => {
        /// Read a `[pipeline.plugin.config]` value injected by the host.
        ///
        /// Available inside any boundary call, `describe` included. Values are
        /// strings; parse numerics yourself or use `saci_config_parse`.
        pub fn saci_config_get(key: &str) -> Option<String> {
            $crate::__rt::config_get(key)
        }

        /// Read and parse a `[pipeline.plugin.config]` value.
        ///
        /// `None` means the key was absent; `Some(Err(_))` means it was present
        /// but did not parse as `T`.
        pub fn saci_config_parse<T: ::core::str::FromStr>(
            key: &str,
        ) -> Option<Result<T, T::Err>> {
            saci_config_get(key).map(|value| value.parse::<T>())
        }

        /// The ABI version this plugin was built against.
        #[unsafe(no_mangle)]
        pub extern "C" fn saci_abi_version() -> u32 {
            $crate::__rt::SACI_ABI_VERSION
        }

        unsafe extern "C-unwind" fn __saci_plugin_describe(
            instance: *mut ::core::ffi::c_void,
            manifest_json: *mut $crate::__rt::SaciBuffer,
            err: *mut $crate::__rt::SaciBuffer,
        ) -> $crate::__rt::SaciStatus {
            unsafe {
                $crate::__rt::describe_impl::<$spec>(
                    instance,
                    $build_fn,
                    ::core::env!("CARGO_PKG_VERSION"),
                    manifest_json,
                    err,
                )
            }
        }

        unsafe extern "C-unwind" fn __saci_plugin_run_batch(
            instance: *mut ::core::ffi::c_void,
            input: $crate::__rt::SaciSlice,
            prior: $crate::__rt::SaciSlice,
            has_prior: i32,
            out: *mut $crate::__rt::SaciRunResult,
            err: *mut $crate::__rt::SaciBuffer,
        ) -> $crate::__rt::SaciStatus {
            unsafe {
                $crate::__rt::run_batch_impl::<$spec>(
                    instance,
                    $build_fn,
                    input,
                    prior,
                    has_prior,
                    out,
                    err,
                )
            }
        }

        unsafe extern "C" fn __saci_plugin_free_buffer(
            _instance: *mut ::core::ffi::c_void,
            buffer: $crate::__rt::SaciBuffer,
        ) {
            unsafe { $crate::__rt::free_buffer(buffer) }
        }

        unsafe extern "C" fn __saci_plugin_destroy(instance: *mut ::core::ffi::c_void) {
            unsafe { $crate::__rt::PluginInstance::from_raw(instance) }
        }

        /// Hand the host this plugin's vtable.
        ///
        /// # Safety
        ///
        /// `host` must be null or point at a valid vtable that outlives the
        /// instance. `out` must point at writable storage for one
        /// `SaciPluginV1`.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn saci_plugin_v1(
            host: *const $crate::__rt::SaciHostV1,
            out: *mut $crate::__rt::SaciPluginV1,
        ) -> $crate::__rt::SaciStatus {
            if out.is_null() {
                return $crate::__rt::SaciStatus::PERMANENT;
            }
            // A null host is tolerated: the plugin then has no config, no
            // logging and no metrics, which is exactly what an empty vtable
            // gives it.
            let host = if host.is_null() {
                $crate::__rt::SaciHostV1::empty()
            } else {
                unsafe { *host }
            };
            let instance = $crate::__rt::PluginInstance::new(host).into_raw();
            unsafe {
                *out = $crate::__rt::SaciPluginV1 {
                    instance,
                    describe: Some(__saci_plugin_describe),
                    run_batch: Some(__saci_plugin_run_batch),
                    free_buffer: Some(__saci_plugin_free_buffer),
                    destroy: Some(__saci_plugin_destroy),
                };
            }
            $crate::__rt::SaciStatus::OK
        }
    };
}
