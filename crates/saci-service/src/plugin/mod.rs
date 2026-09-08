//! Native plugin host: driving a shared library as a pipeline runtime.
//!
//! A native plugin is a `.so`, `.dll` or `.dylib` exporting the C ABI in
//! `saci-plugin-abi`. [`NativePluginRuntime`] loads one, asks it to describe
//! itself, and then implements [`saci_core::runtime::PipelineRuntime`] on top of
//! its `run_batch` call. Arrow IPC bytes carry the dataset in and out, and one
//! opaque blob carries whatever state the plugin needs across batches.
//!
//! This mirrors the WebAssembly path in [`crate::wasm`] call for call, so the
//! two runtimes stay interchangeable behind the trait. The difference is trust:
//! a plugin runs in-process with full host privileges and cannot be preempted,
//! so a wedged plugin wedges its caller.

mod host_impl;
mod loader;
mod manifest;
mod runtime;

pub use runtime::{NativePluginRuntime, PluginBatchMetrics};
