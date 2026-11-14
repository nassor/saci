//! SACI: a batch processing engine on Apache Arrow.
//!
//! One crate, compiled to what its features select. `use saci::prelude::*;`
//! is the one import for whichever combination is enabled.
//!
//! - `engine` (default): the columnar engine. `Dataset`, `Pipeline`,
//!   `System`, `Scheduler`, `Component`, and the `Source`/`Sink` traits in
//!   [`io`].
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
//! - `processor`: the WebAssembly Component Model processor SDK in
//!   `processor`, with the `#[derive(Component)]`, `#[transform]`, `#[fold]`
//!   and `#[processor]` macros and `export_pipeline!`. A processor targets
//!   wasm32-wasip2, where tokio cannot build, so it depends on this crate
//!   with `default-features = false, features = ["processor"]`.
//! - `plugin`: the native plugin SDK in `plugin` and `export_plugin!`, for a
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
pub mod partition;
pub mod pipeline;
pub mod resource;
pub mod row;
pub mod scheduler;
pub mod schema;
pub mod sdk;
pub mod system;

#[cfg(feature = "runtime")]
pub mod runtime;

#[cfg(feature = "io")]
pub mod io;

/// The `window` geometry enum. Declared in every build: a host parses a
/// window declaration whether or not it carries the engine that serves one.
pub mod window_spec;

#[cfg(feature = "windows")]
pub mod windows;

pub use error::{SaciError, SaciResult};
pub use retry::{RetryMode, SystemConfig};

pub use component::Component;
pub use dataset::{Dataset, DatasetBuilder};
pub use partition::KeyPartition;
pub use pipeline::{Pipeline, PipelineBuilder, RunStats};
pub use row::Row;
pub use scheduler::{BackpressureSpec, DependencyKind, PipelineConfig, Scheduler};
pub use schema::SchemaRegistry;
pub use system::{
    FieldAccess, FieldRef, ParallelSystem, ResourceUpdate, SliceWriteSet, System, SystemMeta,
    WriteSet, system_fn,
};

#[cfg(feature = "runtime")]
pub use runtime::{PipelineRuntime, RuntimeDescriptorInfo, RuntimeOutput};

#[cfg(feature = "config")]
pub mod config;

#[cfg(feature = "transformer-contract")]
pub mod transformer;

#[cfg(feature = "connector-contract")]
pub mod connector;

#[cfg(feature = "processor")]
pub mod processor;

#[cfg(feature = "plugin")]
pub mod plugin;

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

#[cfg(feature = "connector-channel")]
pub use connector::channel::{ChannelRegistry, ChannelSink, ChannelSource};

#[cfg(feature = "connector-file")]
pub use connector::file::{FileSink, FileSource};

#[cfg(feature = "connector-redb")]
pub use connector::redb::{RedbSink, RedbSinkConfig, RedbSource, RedbSourceConfig};

#[cfg(feature = "connector-http")]
pub use connector::http::{
    ContentEncoding, HttpClientOptions, HttpPushOptions, HttpPushSource, HttpSink, HttpSinkOptions,
    HttpSource, HttpSourceOptions, HttpVersion, Pagination, ReadMode, SchemaFrom, ServerTls,
    TlsFiles, Window,
};

#[cfg(feature = "connector-tcp")]
pub use connector::tcp::{TcpIngestSource, TcpSink};

#[cfg(feature = "connector-udp")]
pub use connector::udp::{UdpSink, UdpSource};

#[cfg(feature = "connector-saci")]
pub use connector::saci::{SaciSink, SaciSource};

#[cfg(feature = "transformer-contract")]
pub use transformer::{BatchReader, BatchWriter, MessageDecoder, Transformer};

#[cfg(feature = "transformer-arrow-ipc")]
pub use transformer::arrow_ipc::ArrowIpcTransformer;

#[cfg(feature = "transformer-avro")]
pub use transformer::avro::AvroTransformer;

#[cfg(feature = "transformer-csv")]
pub use transformer::csv::CsvTransformer;

#[cfg(feature = "transformer-ndjson")]
pub use transformer::ndjson::NdjsonTransformer;

#[cfg(feature = "transformer-parquet")]
pub use transformer::parquet::ParquetTransformer;

// `ProcessorState` and `RouteDecision` cross a batch boundary for `processor`
// and `plugin` alike.
#[cfg(any(feature = "processor", feature = "plugin"))]
pub use sdk::{ProcessorState, RouteDecision};

#[cfg(feature = "processor")]
pub use processor::{Config, Error, Result};

/// Convenience re-exports of the most commonly used types and traits.
///
/// `use saci::prelude::*;` pulls in the engine types plus whatever the
/// enabled `processor`, `connector-*` and `transformer-*` features add.
pub mod prelude {
    #[cfg(feature = "runtime")]
    pub use crate::runtime::RuntimeOutput;
    pub use crate::{
        BackpressureSpec, Component, Dataset, DependencyKind, FieldAccess, KeyPartition,
        ParallelSystem, Pipeline, PipelineBuilder, PipelineConfig, ResourceUpdate, RetryMode, Row,
        RunStats, SaciError, SaciResult, Scheduler, SchemaRegistry, SliceWriteSet, System,
        SystemConfig, SystemMeta, WriteSet, system_fn,
    };

    pub use crate::column::ComponentView;
    pub use crate::dataset::DatasetBuilder;
    pub use crate::system::FieldRef;

    pub use async_trait::async_trait;

    #[cfg(feature = "windows")]
    pub use crate::windows::{CURRENT_ACCUMULATOR_VERSION, WindowAccumulator, WindowWatermark};

    #[cfg(feature = "processor")]
    pub use crate::processor::{Config, Error, Result};

    #[cfg(feature = "connector-channel")]
    pub use crate::{ChannelRegistry, ChannelSink, ChannelSource};

    #[cfg(feature = "connector-file")]
    pub use crate::{FileSink, FileSource};

    #[cfg(feature = "connector-redb")]
    pub use crate::{RedbSink, RedbSource};

    #[cfg(feature = "connector-http")]
    pub use crate::{HttpPushSource, HttpSink, HttpSource};

    #[cfg(feature = "connector-tcp")]
    pub use crate::{TcpIngestSource, TcpSink};

    #[cfg(feature = "connector-udp")]
    pub use crate::{UdpSink, UdpSource};

    #[cfg(feature = "connector-saci")]
    pub use crate::{SaciSink, SaciSource};

    #[cfg(feature = "transformer-contract")]
    pub use crate::Transformer;

    #[cfg(feature = "transformer-arrow-ipc")]
    pub use crate::ArrowIpcTransformer;

    #[cfg(feature = "transformer-avro")]
    pub use crate::AvroTransformer;

    #[cfg(feature = "transformer-csv")]
    pub use crate::CsvTransformer;

    #[cfg(feature = "transformer-ndjson")]
    pub use crate::NdjsonTransformer;

    #[cfg(feature = "transformer-parquet")]
    pub use crate::ParquetTransformer;
}
