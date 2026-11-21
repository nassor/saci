//! saci-service: SACI distributed batch processing service binary.
//!
//! The whole command line is [`saci::service::cli::main`]; this binary adds
//! only the allocator.
//!
//! ## Usage
//!
//! ```text
//! saci-service serve --config service.kdl
//! saci-service validate --config service.kdl
//! saci-service status --url http://localhost:8080
//! saci-service cluster init --config service.kdl
//! saci-service cluster status --url http://localhost:8080
//! ```

// Pipelines allocate and free multi-megabyte Arrow arrays once per batch.
// System allocators return those large blocks to the OS on free, so the next
// batch soft-faults every page again; mimalloc retains them instead.
//
// Leave `mimalloc` out of the feature set to opt out; the library itself
// never installs an allocator.
#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> std::process::ExitCode {
    saci::service::cli::main()
}
