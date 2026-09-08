//! `saci-connector-channel`: an in-memory [`Source`] and [`Sink`] for SACI.
//!
//! [`Source`]: saci_core::io::source::Source
//! [`Sink`]: saci_core::io::sink::Sink
//!
//! [`ChannelSource`] and [`ChannelSink`] carry `RecordBatch`es over a tokio mpsc
//! channel, so a pipeline runs end to end without file IO. Each can be built as
//! a standalone pair with their other half (`new`), or resolved by name through
//! a shared [`ChannelRegistry`]: a `ChannelSink` in one workflow and a
//! `ChannelSource` in another meet on one `mpsc` pair, so a sink's producer
//! finishing (dropping the sink) is what signals the paired source's EOF.
//!
//! Their factories take `name` (the channel to resolve through the
//! registered [`saci_connector::ChannelBridge`]), `buffer` (mpsc capacity,
//! default 8) and a `schema_fields` array.

pub mod factory;
pub mod registry;
pub mod sink;
pub mod source;

pub use factory::{ChannelSinkFactory, ChannelSourceFactory};
pub use registry::ChannelRegistry;
pub use sink::ChannelSink;
pub use source::ChannelSource;
