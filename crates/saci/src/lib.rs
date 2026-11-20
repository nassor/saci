//! SACI: a batch processing engine on Apache Arrow.
//!
//! One crate, compiled to what its features select. `use saci::prelude::*;`
//! is the one import for whichever combination is enabled.
//!
//! - `engine` (default): the columnar engine. `Dataset`, `Pipeline`,
//!   `System`, `Scheduler`, `Component`, the `Source`/`Sink` traits in
//!   [`io`], and tokio and rayon stage parallelism.
//! - `derive`: `#[derive(Component)]`, included in `engine`, `processor` and
//!   `plugin`.
//! - `windows`: Beam-style windowed aggregation in `windows`.
//! - `connector-<name>`: one connector in `connector::<name>`. `connectors`
//!   enables the seven that need nothing installed or already running:
//!   `channel`, `file`, `redb`, `http`, `tcp`, `udp` and `saci`. `kafka`,
//!   `mariadb`, `nats`, `postgresql`, `s3`, `turso`, `clickhouse` and
//!   `opentelemetry` each need a specific broker, database, object store or
//!   OTLP endpoint running first.
//! - `transformer-<format>`: one byte format in `transformer::<format>`.
//!   `transformers` enables all five: `arrow-ipc`, `avro`, `csv`, `ndjson`
//!   and `parquet`.
//! - `processor`: a WebAssembly Component Model processor built with the
//!   authoring SDK in [`sdk`] and `#[processor]`. A processor targets
//!   wasm32-wasip3, where tokio cannot build, so it depends on this crate with
//!   `default-features = false, features = ["processor"]`.
//! - `plugin`: a native plugin built with [`sdk`] and `#[plugin]`, for a
//!   cdylib the host `dlopen`s.
//! - `service`: the host in `service`: KDL configuration, the factory
//!   Registry, the standalone and distributed runners, the HTTP control
//!   plane and the `saci-service` binary. `wasm-host` and `plugin-host` add
//!   the two processor hosts, `service-cluster` a Raft cluster node.
//!   `server` is the bundle `cargo install saci --features server` builds:
//!   the service, the WebAssembly host, the seven connectors above and every
//!   byte format.
//! - `all`: every feature above in one build.
//!
//! A custom server is the same binary with a different feature set, or a
//! `main.rs` that calls into [`service`].
//!
//! ```ignore
//! use saci::prelude::*;
//! ```

// Compiles and runs the README's example under `cargo test --doc`, so the
// crates.io page cannot drift from the API.
#[cfg(all(doctest, feature = "engine"))]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

pub use arrow_array;
pub use arrow_schema;

pub mod error;
pub mod retry;

pub mod column;
pub mod component;
pub mod dataset;
mod partition;
pub mod pipeline;
mod resource;
pub mod row;
pub mod scheduler;
pub mod schema;
pub mod sdk;
pub mod system;

#[cfg(feature = "engine")]
pub mod runtime;

#[cfg(feature = "engine")]
pub mod io;

/// The `window` geometry enum. Declared in every build: a host parses a
/// window declaration whether or not it carries the engine that serves one.
pub mod window_spec;

#[cfg(feature = "windows")]
pub mod windows;

pub use async_trait::async_trait;
pub use component::Component;
pub use dataset::Dataset;
pub use error::{SaciError, SaciResult};
pub use partition::KeyPartition;
pub use pipeline::{Pipeline, RunStats};
pub use retry::RetryMode;
pub use row::Row;
#[cfg(feature = "derive")]
pub use saci_macros::Component;
pub use scheduler::{PipelineConfig, Scheduler};
pub use schema::SchemaRegistry;
pub use system::{
    FieldRef, ParallelSystem, SliceWriteSet, System, SystemMeta, WriteSet, parallel_fn, system_fn,
};

#[cfg(feature = "engine")]
pub use io::{Sink, Source};
#[cfg(feature = "engine")]
pub use runtime::{PipelineRuntime, RuntimeDescriptorInfo, RuntimeOutput};

/// What macro expansions name. Not part of the API.
#[doc(hidden)]
pub mod __private {
    pub use crate::sdk::glue::{
        BatchError, BatchMetrics, BatchOutput, ComponentSchema, Descriptor, HostIo, HostScope,
        PipelineSlot, describe, fingerprint_hex, install_host, run_batch, schema_to_ipc_bytes,
    };
    pub use crate::sdk::host::Level;
    pub use crate::sdk::{Config, Error, RouteDecision, State};
    pub use crate::{
        Component, Dataset, FieldRef, Pipeline, SaciError, SaciResult, System, SystemMeta,
    };
    pub use arrow_schema::{self, Schema};
    pub use async_trait::async_trait;
    pub use serde_arrow;
    pub use std::sync::LazyLock;

    /// The native plugin boundary `#[plugin]` expands to.
    #[cfg(feature = "plugin")]
    pub mod plugin {
        pub use crate::sdk::plugin_rt::{
            PluginInstance, SACI_ABI_VERSION, SaciBuffer, SaciHostV1, SaciPluginV1, SaciRunResult,
            SaciSlice, SaciStatus, describe_impl, free_buffer, run_batch_impl,
        };
    }
}

#[cfg(feature = "config")]
pub mod config;

#[cfg(feature = "transformer-contract")]
pub mod transformer;

#[cfg(feature = "connector-contract")]
pub mod connector;

#[cfg(feature = "plugin")]
pub use saci_macros::plugin;
#[cfg(feature = "processor")]
pub use saci_macros::processor;
#[cfg(feature = "derive")]
pub use saci_macros::{fold, transform};

#[cfg(any(feature = "plugin", feature = "plugin-host"))]
pub mod plugin_abi;

#[cfg(feature = "metrics")]
pub mod inspector_wire;

// The host's OpenTelemetry instruments. The no-op impl behind a disabled
// `metrics` keeps every call site free of `#[cfg]`.
#[cfg(any(
    feature = "metrics",
    feature = "distributed",
    feature = "wasm-host",
    feature = "plugin-host",
    feature = "service"
))]
pub mod metrics;

// In-process telemetry. Sibling of `metrics`, not part of `service`, so a
// library embedder can capture spans and samples without the axum control
// plane.
#[cfg(feature = "inspector")]
pub mod inspector;

#[cfg(feature = "distributed")]
pub mod distributed;

// Shared by the two out-of-process pipeline runtimes: both decode declared
// component schemas into a template dataset the same way.
#[cfg(any(feature = "wasm-host", feature = "plugin-host"))]
mod descriptor;

#[cfg(feature = "wasm-host")]
pub mod wasm_host;

#[cfg(feature = "plugin-host")]
pub mod plugin_host;

#[cfg(feature = "service")]
pub mod service;

/// Convenience re-exports of the most commonly used types and traits.
///
/// `use saci::prelude::*;` pulls in the engine types plus whatever the
/// enabled `processor`, `connector-*` and `transformer-*` features add.
pub mod prelude {
    pub use crate::column::ComponentView;
    #[cfg(feature = "plugin")]
    pub use crate::plugin;
    #[cfg(feature = "processor")]
    pub use crate::processor;
    pub use crate::sdk;
    pub use crate::{
        Component, Dataset, FieldRef, KeyPartition, ParallelSystem, Pipeline, PipelineConfig,
        RetryMode, Row, RunStats, SaciError, SaciResult, Scheduler, SchemaRegistry, SliceWriteSet,
        System, SystemMeta, WriteSet, async_trait, parallel_fn, system_fn,
    };
    #[cfg(feature = "engine")]
    pub use crate::{PipelineRuntime, RuntimeDescriptorInfo, RuntimeOutput, Sink, Source};
    #[cfg(feature = "derive")]
    pub use crate::{fold, transform};

    #[cfg(feature = "windows")]
    pub use crate::windows::{CURRENT_ACCUMULATOR_VERSION, WindowAccumulator, WindowWatermark};
}
