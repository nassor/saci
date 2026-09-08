//! Embed SACI as a pure-Rust library, with no external database or message
//! broker.
//!
//! `use saci::prelude::*;` is the one import for whichever combination of
//! feature groups is enabled below. This crate itself defines no types: it
//! re-exports the workspace crates that make up each group.
//!
//! - `engine` (default): the columnar engine, `Dataset`, `Pipeline`,
//!   `System`, `Scheduler`, `Component`, and the `Source`/`Sink` traits.
//! - `connector-channel`, `connector-file`, `connector-redb`,
//!   `connector-http`, `connector-tcp`, `connector-saci`,
//!   `connector-datafusion` (`connectors` enables all seven): the connectors
//!   that need nothing installed or already running. A channel inside the
//!   process, local disk, an embedded redb key/value file, an HTTP client, a
//!   raw socket, a peer SACI service, and a DataFusion session the caller
//!   owns; HTTP, TCP and the peer link speak the network and depend on no
//!   particular server product.
//!   `saci-connector-kafka`, `-nats`, `-postgresql`, `-s3` and `-turso` each
//!   require a specific broker, database or object store installed and
//!   running first, and this crate carries none of them.
//!   `connector-datafusion` needs a `datafusion::prelude::SessionContext`
//!   the caller builds and owns; add the pinned `datafusion = "55"` directly
//!   for that type.
//! - `transformer-arrow-ipc`, `transformer-avro`, `transformer-csv`,
//!   `transformer-ndjson`, `transformer-parquet` (`transformers` enables all
//!   five): the byte formats a connector reads and writes.
//! - `windows`: Beam-style windowed aggregation, forwarded to whichever of
//!   `engine`, `processor` or `plugin` is enabled.
//! - `processor`: `Pipeline`/`System`/`Component` (hand-written, not
//!   derived), `Config`/`Error`/`Result` and `export_pipeline!`, for a
//!   WebAssembly Component Model processor. A processor targets
//!   wasm32-wasip2 and must not pull tokio in, so build with
//!   `--no-default-features --features processor`. Does not carry the
//!   `#[derive(Component)]`/`#[transform]`/`#[fold]`/`#[processor]` macros:
//!   their expansions name `saci-processor` literally, which only resolves
//!   as a direct dependency of the crate invoking them, not a transitive
//!   one reached through this facade. A processor crate that wants those
//!   macros depends on `saci-processor` directly.
//! - `plugin`: `Pipeline`/`System`/`Component` again, `ProcessorState`,
//!   `RouteDecision` and `export_plugin!`, for a native plugin: a cdylib
//!   the host `dlopen`s.
//! - `all`: every feature this crate carries, `engine`, `windows`,
//!   `connectors`, `transformers`, `processor` and `plugin`, in one
//!   build, for a host-side embedder that wants one dependency. `all`
//!   reaches saci-core's `runtime` feature through `engine`, and tokio
//!   cannot target wasm32-wasip2, so a wasm processor crate still
//!   builds with `--no-default-features --features processor`.
//!
//! `engine`, `processor` and `plugin` each enable `dep:saci-core` directly,
//! so every name above that saci-core declares unconditionally (regardless
//! of its own `runtime`/`processor` feature) is one `pub use` shared by all
//! three, with nothing to reconcile between them: they are the identical
//! types either way. `engine`, `processor` and `plugin` are still not meant
//! to be combined in one build: each authors a different artifact, a host
//! binary, a wasm component, or a native shared library. Doing so is not
//! ambiguous, only redundant.
//!
//! ```ignore
//! use saci::prelude::*;
//! ```

pub use arrow_array;
pub use arrow_schema;

// Mirrors `saci_service`'s crate root: module paths plus the flattened
// types, so a crate written against `saci_service::{component::Component,
// dataset::Dataset, ...}` ports to `saci::{component::Component,
// dataset::Dataset, ...}` with no other changes. Every name below is
// declared unconditionally in saci-core regardless of which of its own
// features (`runtime` or `processor`) ends up active, so one `pub use`
// covers `engine`, `processor` and `plugin` alike.
#[cfg(any(feature = "engine", feature = "processor", feature = "plugin"))]
pub use saci_core::{
    column, component, dataset, error, partition, pipeline, resource, retry, row, scheduler,
    schema, system,
};

#[cfg(any(feature = "engine", feature = "processor", feature = "plugin"))]
pub use saci_core::{
    BackpressureSpec, Component, Dataset, DependencyKind, FieldAccess, KeyPartition,
    ParallelSystem, Pipeline, PipelineBuilder, PipelineConfig, ResourceUpdate, RetryMode, Row,
    RunStats, SaciError, SaciResult, Scheduler, SchemaRegistry, SliceWriteSet, System,
    SystemConfig, SystemMeta, WriteSet, system_fn,
};

// `io` (the Source/Sink traits, drain_into_dataset, drain_dataset, the cast
// helpers) needs saci-core's own `io` feature, which only `engine`'s entry
// below requests. `processor` avoids it deliberately: `io` implies
// `runtime` (tokio, rayon), which cannot target wasm32-wasip2. `plugin`
// already carries `runtime` through saci-plugin's own saci-core edge (its
// own default features), but a plugin authors a batch processor, not a
// host-level connector, so it has no use for `io` either way.
#[cfg(feature = "engine")]
pub use saci_core::io;

#[cfg(all(
    any(feature = "engine", feature = "processor", feature = "plugin"),
    feature = "windows"
))]
pub use saci_core::windows;

#[cfg(feature = "connector-channel")]
pub use saci_connector_channel::{ChannelRegistry, ChannelSink, ChannelSource};

#[cfg(feature = "connector-file")]
pub use saci_connector_file::{FileSink, FileSource};

#[cfg(feature = "connector-redb")]
pub use saci_connector_redb::{RedbSink, RedbSinkConfig, RedbSource, RedbSourceConfig};

#[cfg(feature = "connector-http")]
pub use saci_connector_http::{HttpSink, HttpSource, SchemaFrom};

#[cfg(feature = "connector-tcp")]
pub use saci_connector_tcp::{TcpIngestSource, TcpSink};

#[cfg(feature = "connector-saci")]
pub use saci_connector_saci::{SaciSink, SaciSource};

#[cfg(feature = "connector-datafusion")]
pub use saci_connector_datafusion::DataFusionSource;

#[cfg(feature = "transformer-contract")]
pub use saci_transformer::{BatchReader, BatchWriter, MessageDecoder, Transformer};

#[cfg(feature = "transformer-arrow-ipc")]
pub use saci_transformer_arrow_ipc::ArrowIpcTransformer;

#[cfg(feature = "transformer-avro")]
pub use saci_transformer_avro::AvroTransformer;

#[cfg(feature = "transformer-csv")]
pub use saci_transformer_csv::CsvTransformer;

#[cfg(feature = "transformer-ndjson")]
pub use saci_transformer_ndjson::NdjsonTransformer;

#[cfg(feature = "transformer-parquet")]
pub use saci_transformer_parquet::ParquetTransformer;

/// Author a WebAssembly Component Model processor pipeline.
///
/// `export_pipeline!` wires a `fn() -> Pipeline` to the `saci:pipeline@0.3.0`
/// WIT world exports; see [`saci_processor`] for the full authoring guide.
/// Safe to re-export: unlike the `#[derive(Component)]`/`#[transform]`/
/// `#[fold]`/`#[processor]` macros this crate deliberately omits (see the
/// `processor` feature's own doc in `Cargo.toml`), `export_pipeline!` is a
/// `macro_rules!` macro whose `$crate`-qualified paths resolve to
/// `saci-processor` regardless of which path the caller invoked it
/// through.
#[cfg(feature = "processor")]
pub use saci_processor::export_pipeline;

/// Author a native plugin: a cdylib the host `dlopen`s.
///
/// `export_plugin!` wires a `fn() -> Pipeline` to the `saci-plugin-abi` C
/// ABI; see [`saci_plugin`] for the full authoring guide.
#[cfg(feature = "plugin")]
pub use saci_plugin::export_plugin;

// `ProcessorState` and `RouteDecision` cross a batch boundary for `processor`
// and `plugin` alike; both SDKs re-export the identical `saci_core::sdk`
// items, so reaching them directly needs no reconciliation between the two
// features either.
#[cfg(any(feature = "processor", feature = "plugin"))]
pub use saci_core::sdk::{ProcessorState, RouteDecision};

// `Config`, `Error` and `Result` are real types `saci-processor` defines
// itself, not re-exports of anything saci-core or saci-plugin also carry,
// so they stay `processor`-only with no shared or excluded name.
#[cfg(feature = "processor")]
pub use saci_processor::{Config, Error, Result};

/// Convenience re-exports of the most commonly used types and traits.
///
/// `use saci::prelude::*;` pulls in whatever the enabled feature groups
/// provide: with `engine`, `processor` or `plugin`, `Dataset`, `Pipeline`,
/// `System`, `Component` (the trait), plus the connector and transformer
/// types the enabled `connector-*`/`transformer-*` features add, and with
/// `processor` also `Config`/`Error`/`Result`. Does not carry
/// `#[derive(Component)]`, `#[transform]`, `#[fold]` or `#[processor]`; see
/// the `processor` feature's doc in `Cargo.toml` for why.
pub mod prelude {
    // Every name `saci_core::prelude` exports is declared unconditionally
    // there too (its own `windows`-gated items follow this crate's own
    // `windows` feature transparently), so one glob, active under any of
    // the three SDK features, replaces what would otherwise be three
    // separate globs needing pairwise exclusion to stay `unused_imports`-
    // clean under `--all-features`.
    #[cfg(any(feature = "engine", feature = "processor", feature = "plugin"))]
    pub use saci_core::prelude::*;

    #[cfg(feature = "processor")]
    pub use saci_processor::{Config, Error, Result};

    #[cfg(feature = "connector-channel")]
    pub use crate::{ChannelRegistry, ChannelSink, ChannelSource};

    #[cfg(feature = "connector-file")]
    pub use crate::{FileSink, FileSource};

    #[cfg(feature = "connector-redb")]
    pub use crate::{RedbSink, RedbSource};

    #[cfg(feature = "connector-http")]
    pub use crate::{HttpSink, HttpSource};

    #[cfg(feature = "connector-tcp")]
    pub use crate::{TcpIngestSource, TcpSink};

    #[cfg(feature = "connector-saci")]
    pub use crate::{SaciSink, SaciSource};

    #[cfg(feature = "connector-datafusion")]
    pub use crate::DataFusionSource;

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
