//! `saci-connector-datafusion`: a DataFusion SQL [`Source`] for SACI.
//!
//! [`Source`]: saci_core::io::source::Source
//!
//! [`DataFusionSource`] runs a SQL statement against a live `SessionContext` and
//! streams the resulting batches into a `Dataset`. Batches are pulled lazily, so
//! DataFusion executes the query as the pipeline consumes it.
//!
//! There is no factory: the source needs a `SessionContext` the caller owns, and
//! the service config cannot express one. Build it in Rust and hand it to the
//! pipeline.

pub mod source;

pub use source::DataFusionSource;
