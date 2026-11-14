//! [`ChannelBridge`]: the in-process channel-pairing contract a
//! `ChannelSource`/`ChannelSink` factory resolves a named half through.

use std::sync::Arc;

use arrow_schema::Schema;

use crate::error::SaciError;
use crate::io::{sink::Sink, source::Source};

/// Resolves a named in-process channel shared across workflows: a
/// `ChannelSink` in one workflow and a `ChannelSource` in another meet on one
/// shared `tokio::sync::mpsc` pair.
///
/// Implemented by `crate::connector::channel::ChannelRegistry`. The trait lives in
/// `crate::connector`, not in the `channel` connector, so the channel factories can
/// name it through [`ConnectorContext`](crate::connector::ConnectorContext) without
/// the `channel` connector depending back on the host that registers the
/// bridge.
pub trait ChannelBridge: Send + Sync + 'static {
    /// The sink half for channel `name`, declared with `schema` and `buffer`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when a `ChannelSink` for `name`
    /// was already built, or when `schema`/`buffer` disagree with the paired
    /// `ChannelSource`'s.
    fn sink(
        &self,
        name: &str,
        schema: Arc<Schema>,
        buffer: usize,
    ) -> Result<Box<dyn Sink>, SaciError>;

    /// The source half for channel `name`, declared with `schema` and
    /// `buffer`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when a `ChannelSource` for `name`
    /// was already built, or when `schema`/`buffer` disagree with the paired
    /// `ChannelSink`'s.
    fn source(
        &self,
        name: &str,
        schema: Arc<Schema>,
        buffer: usize,
    ) -> Result<Box<dyn Source>, SaciError>;
}
