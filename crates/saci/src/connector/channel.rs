//! The channel connector: an in-memory [`Source`] and [`Sink`] for SACI.
//!
//! [`Source`]: crate::io::Source
//! [`Sink`]: crate::io::Sink
//!
//! [`ChannelSource`] and [`ChannelSink`] carry `RecordBatch`es over a tokio mpsc
//! channel, so a pipeline runs end to end without file IO. Each can be built as
//! a standalone pair with their other half (`new`), or resolved by name through
//! a shared [`ChannelRegistry`]: a `ChannelSink` in one workflow and a
//! `ChannelSource` in another meet on one `mpsc` pair, so a sink's producer
//! finishing (dropping the sink) is what signals the paired source's EOF.
//!
//! The [`ChannelConnector`] takes `name` (the channel to resolve through the
//! registered [`crate::connector::ChannelBridge`]), `buffer` (mpsc capacity,
//! default 8, at least 1) and `field` entries.

/// The name this connector registers under and a config's `connector` property selects.
pub const NAME: &str = "channel";

pub mod factory;
pub mod registry;
pub mod sink;
pub mod source;

pub use factory::ChannelConnector;
pub use registry::ChannelRegistry;
pub use sink::ChannelSink;
pub use source::ChannelSource;
