// `path` is `CARGO_MANIFEST_DIR`-relative: the canonical `wit/pipeline.wit`
// the processor SDK's guests generate their bindings against too.
wasmtime::component::bindgen!({
    world: "saci-pipeline",
    path: "wit",
});

// Convenience re-exports used across the wasm module. `self::` because the
// generated `saci` module would otherwise be ambiguous with this crate's own
// name, which rustdoc passes as an extern when it builds the doctests.
#[allow(unused_imports)]
pub use self::saci::pipeline::host_io::{Host as HostIo, LogLevel};
#[allow(unused_imports)]
pub use self::saci::pipeline::types::{
    ComponentDescriptor, PipelineDescriptor, RunError, RunResult,
};
// The `types` WIT interface generates an empty Host marker trait that every
// store-data type must implement alongside HostIo.
pub use self::saci::pipeline::types::Host as TypesHost;
