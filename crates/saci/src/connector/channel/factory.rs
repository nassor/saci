//! The channel source and sink factories.
//!
//! `ChannelSource` and `ChannelSink` are in-memory, mpsc-backed IO endpoints.
//! Both resolve their half of a named channel through the host's registered
//! [`crate::connector::ChannelBridge`]: a `ChannelSink` in one workflow and a
//! `ChannelSource` in another meet on one shared `mpsc` pair, so the
//! producer's sink dropping is what signals the consumer's source EOF.
//!
//! Both take `name` (required, the channel to resolve through the bridge),
//! `buffer` (mpsc capacity, default 8) and `schema_fields` in their `config`
//! table.

use serde::Deserialize;

use crate::connector::schema::{at_least_one, deserialize_config};
use crate::connector::{
    ConfigValue, ConnectorContext, SinkFactory, SourceFactory, parse_schema_fields,
};
use crate::error::SaciError;
use crate::io::{Sink, Source};

/// Why neither half can be built a second time, reported by
/// [`SourceFactory::rebuildable`] and [`SinkFactory::rebuildable`].
const CHANNEL_NOT_REBUILDABLE: &str =
    "a channel is bound to one mpsc pair created once per process";

/// Default mpsc capacity.
const DEFAULT_BUFFER: usize = 8;

fn default_buffer() -> usize {
    DEFAULT_BUFFER
}

/// The `config` table both halves of a channel read: the shared name and the
/// mpsc capacity. `schema_fields` is read beside it.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChannelConfig {
    name: String,
    #[serde(default = "default_buffer")]
    buffer: usize,
}

impl ChannelConfig {
    /// Read `config` for the half named `what`, refusing a zero capacity:
    /// an mpsc channel of none panics instead of blocking.
    fn read(config: &ConfigValue, what: &str) -> Result<Self, SaciError> {
        let cfg: Self = deserialize_config(config, what)?;
        at_least_one("channel", "buffer", cfg.buffer as u64)?;
        Ok(cfg)
    }
}

/// Factory for [`ChannelSource`](crate::connector::channel::ChannelSource).
///
/// Resolves the source half of the named channel through the host's
/// registered [`crate::connector::ChannelBridge`].
///
/// The `config` table is a `name` (string, required: the channel name, shared
/// with the paired `ChannelSink`) and a `buffer` (usize, optional, default
/// `8`, at least `1`: mpsc channel capacity). Beside it, `schema_fields`
/// (list, required) is the Arrow schema definition.
pub struct ChannelSourceFactory;

impl SourceFactory for ChannelSourceFactory {
    fn type_name(&self) -> &'static str {
        "ChannelSource"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        let bridge = ctx.channels().ok_or_else(|| {
            SaciError::configuration(
                "ChannelSource requires a channel bridge; register one via \
                 ServiceBuilder::with_channel_bridge",
            )
        })?;
        let cfg = ChannelConfig::read(config, "ChannelSource")?;
        let schema = parse_schema_fields(config, "ChannelSource")?;
        bridge.source(&cfg.name, schema, cfg.buffer)
    }

    /// `ChannelRegistry` latches each named half as it is built and refuses a
    /// second one, so a rebuild could never produce an instance.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Err(CHANNEL_NOT_REBUILDABLE)
    }
}

/// Factory for [`ChannelSink`](crate::connector::channel::ChannelSink).
///
/// Resolves the sink half of the named channel through the host's registered
/// [`crate::connector::ChannelBridge`].
///
/// The `config` table is a `name` (string, required: the channel name, shared
/// with the paired `ChannelSource`) and a `buffer` (usize, optional, default
/// `8`, at least `1`: mpsc channel capacity). Beside it, `schema_fields`
/// (list, required) is the Arrow schema definition.
pub struct ChannelSinkFactory;

impl SinkFactory for ChannelSinkFactory {
    fn type_name(&self) -> &'static str {
        "ChannelSink"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError> {
        let bridge = ctx.channels().ok_or_else(|| {
            SaciError::configuration(
                "ChannelSink requires a channel bridge; register one via \
                 ServiceBuilder::with_channel_bridge",
            )
        })?;
        let cfg = ChannelConfig::read(config, "ChannelSink")?;
        let schema = parse_schema_fields(config, "ChannelSink")?;
        bridge.sink(&cfg.name, schema, cfg.buffer)
    }

    /// `ChannelRegistry` latches each named half as it is built and refuses a
    /// second one, and the sink being the channel's only `Sender` is what
    /// gives the consumer a real EOF.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Err(CHANNEL_NOT_REBUILDABLE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::{ConfigMap, from_kdl_str};
    use std::sync::Arc;

    use crate::connector::channel::ChannelRegistry;

    fn schema_config() -> ConfigValue {
        from_kdl_str(
            r#"
name "bridge"
buffer 4

schema_fields "id" type="Int64" nullable=#false
"#,
        )
        .unwrap()
    }

    fn ctx_with_bridge() -> ConnectorContext {
        ConnectorContext::new(None).with_channels(Arc::new(ChannelRegistry::default()))
    }

    #[test]
    fn test_channel_source_factory_type_name() {
        assert_eq!(ChannelSourceFactory.type_name(), "ChannelSource");
    }

    #[test]
    fn test_channel_sink_factory_type_name() {
        assert_eq!(ChannelSinkFactory.type_name(), "ChannelSink");
    }

    #[test]
    fn test_channel_source_and_sink_pair_through_the_bridge() {
        let ctx = ctx_with_bridge();
        let sink = ChannelSinkFactory.build(&schema_config(), &ctx).unwrap();
        let src = ChannelSourceFactory.build(&schema_config(), &ctx).unwrap();
        assert_eq!(src.schema().fields().len(), 1);
        assert_eq!(sink.schema().fields().len(), 1);
    }

    #[test]
    fn test_channel_source_missing_schema_fields_returns_error() {
        let ctx = ctx_with_bridge();
        let mut config = ConfigMap::new();
        config.insert(
            "name".to_string(),
            ConfigValue::String("bridge".to_string()),
        );
        let err = ChannelSourceFactory
            .build(&ConfigValue::Object(config), &ctx)
            .err()
            .expect("should return error");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("schema_fields"));
    }

    #[test]
    fn test_channel_source_missing_name_returns_error() {
        let ctx = ctx_with_bridge();
        let raw = from_kdl_str("schema_fields \"id\" type=\"Int64\" nullable=#false\n").unwrap();
        let err = ChannelSourceFactory
            .build(&raw, &ctx)
            .err()
            .expect("should return error");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("`name`"));
    }

    #[test]
    fn test_channel_sink_missing_name_returns_error() {
        let ctx = ctx_with_bridge();
        let raw = from_kdl_str("schema_fields \"id\" type=\"Int64\" nullable=#false\n").unwrap();
        let err = ChannelSinkFactory
            .build(&raw, &ctx)
            .err()
            .expect("should return error");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("`name`"));
    }

    #[test]
    fn test_channel_source_without_a_registered_bridge_returns_error() {
        let err = ChannelSourceFactory
            .build(&schema_config(), &ConnectorContext::new(None))
            .err()
            .expect("should return error");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("channel bridge"));
    }

    #[test]
    fn test_channel_sink_without_a_registered_bridge_returns_error() {
        let err = ChannelSinkFactory
            .build(&schema_config(), &ConnectorContext::new(None))
            .err()
            .expect("should return error");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("channel bridge"));
    }

    /// An mpsc channel of capacity zero panics, so a zero is refused while
    /// the config is read rather than reaching the bridge.
    #[test]
    fn test_a_zero_buffer_is_a_configuration_error_not_a_panic() {
        let ctx = ctx_with_bridge();
        let raw = from_kdl_str(
            "name \"bridge\"\nbuffer 0\nschema_fields \"id\" type=\"Int64\" nullable=#false\n",
        )
        .unwrap();

        let err = ChannelSourceFactory
            .build(&raw, &ctx)
            .err()
            .expect("a zero buffer must be refused");
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("'buffer' must be at least 1"),
            "got: {err}"
        );

        let err = ChannelSinkFactory
            .build(&raw, &ctx)
            .err()
            .expect("a zero buffer must be refused");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'buffer' must be at least 1"));
    }

    #[test]
    fn test_a_misspelled_key_is_named() {
        let ctx = ctx_with_bridge();
        let raw = from_kdl_str(
            "name \"bridge\"\nbufer 4\nschema_fields \"id\" type=\"Int64\" nullable=#false\n",
        )
        .unwrap();
        let err = ChannelSourceFactory
            .build(&raw, &ctx)
            .err()
            .expect("an unknown key must be refused");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("bufer"), "got: {err}");
    }
}
