//! The channel [`Connector`].
//!
//! `ChannelSource` and `ChannelSink` are in-memory, mpsc-backed IO endpoints.
//! Both resolve their half of a named channel through the host's registered
//! [`crate::connector::ChannelBridge`]: a `ChannelSink` in one workflow and a
//! `ChannelSource` in another meet on one shared `mpsc` pair, so the
//! producer's sink dropping is what signals the consumer's source EOF.
//!
//! Both take `name` (required, the channel to resolve through the bridge),
//! `buffer` (mpsc capacity, default 8) and `field` entries in their `config`
//! table.

use serde::Deserialize;

use crate::connector::schema::{at_least_one, deserialize_config};
use crate::connector::{
    ConfigValue, Connector, ConnectorContext, Rebuild, Role, parse_fields, schema_of,
};
use crate::error::{SaciError, SaciResult};
use crate::io::{Sink, Source};

/// Why neither half can be built a second time, reported by
/// [`Connector::rebuild`].
const CHANNEL_NOT_REBUILDABLE: &str =
    "a channel is bound to one mpsc pair created once per process";

/// Default mpsc capacity.
const DEFAULT_BUFFER: usize = 8;

fn default_buffer() -> usize {
    DEFAULT_BUFFER
}

/// The `config` table both halves of a channel read: the shared name and the
/// mpsc capacity. `field` is read beside it.
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

/// The in-memory channel connector: a `channel` node of either role.
///
/// Resolves the matching half of the named channel through the host's
/// registered [`crate::connector::ChannelBridge`]; the source half is a
/// [`ChannelSource`](crate::connector::channel::ChannelSource) and the sink
/// half a [`ChannelSink`](crate::connector::channel::ChannelSink).
///
/// The `config` table of both roles is a `name` (string, required: the
/// channel name, shared by the paired source and sink) and a `buffer` (usize,
/// optional, default `8`, at least `1`: mpsc channel capacity). Beside it,
/// `field` (one or more, required) declares the Arrow schema.
pub struct ChannelConnector;

impl Connector for ChannelConnector {
    fn name(&self) -> &'static str {
        "channel"
    }

    fn source(&self, config: &ConfigValue, ctx: &ConnectorContext) -> SaciResult<Box<dyn Source>> {
        let bridge = ctx.channels().ok_or_else(|| {
            SaciError::configuration(
                "ChannelSource requires a channel bridge; register one via \
                 ServiceBuilder::channels",
            )
        })?;
        let cfg = ChannelConfig::read(config, "ChannelSource")?;
        let schema = schema_of(&parse_fields(config, "ChannelSource")?)?;
        bridge.source(&cfg.name, schema, cfg.buffer)
    }

    fn sink(&self, config: &ConfigValue, ctx: &ConnectorContext) -> SaciResult<Box<dyn Sink>> {
        let bridge = ctx.channels().ok_or_else(|| {
            SaciError::configuration(
                "ChannelSink requires a channel bridge; register one via \
                 ServiceBuilder::channels",
            )
        })?;
        let cfg = ChannelConfig::read(config, "ChannelSink")?;
        let schema = schema_of(&parse_fields(config, "ChannelSink")?)?;
        bridge.sink(&cfg.name, schema, cfg.buffer)
    }

    /// `ChannelRegistry` latches each named half as it is built and refuses a
    /// second one, so a rebuild could never produce an instance; the sink
    /// being the channel's only `Sender` is also what gives the consumer a
    /// real EOF.
    fn rebuild(&self, _role: Role, _config: &ConfigValue) -> Rebuild {
        Rebuild::No(CHANNEL_NOT_REBUILDABLE)
    }

    fn detail_keys(&self, _role: Role) -> &'static [&'static str] {
        &["name"]
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

field "id" type="int64" nullable=#false
"#,
        )
        .unwrap()
    }

    fn ctx_with_bridge() -> ConnectorContext {
        ConnectorContext::new(None).with_channels(Arc::new(ChannelRegistry::default()))
    }

    #[test]
    fn test_channel_connector_name_roles_and_detail_keys() {
        assert_eq!(ChannelConnector.name(), "channel");
        assert_eq!(ChannelConnector.roles(), &[Role::Source, Role::Sink]);
        assert_eq!(ChannelConnector.detail_keys(Role::Source), &["name"]);
        assert_eq!(ChannelConnector.detail_keys(Role::Sink), &["name"]);
        assert!(!ChannelConnector.live(&schema_config()));
    }

    #[test]
    fn test_neither_channel_half_is_rebuildable() {
        let cfg = schema_config();
        assert_eq!(
            ChannelConnector.rebuild(Role::Source, &cfg),
            Rebuild::No(CHANNEL_NOT_REBUILDABLE)
        );
        assert_eq!(
            ChannelConnector.rebuild(Role::Sink, &cfg),
            Rebuild::No(CHANNEL_NOT_REBUILDABLE)
        );
    }

    #[test]
    fn test_channel_source_and_sink_pair_through_the_bridge() {
        let ctx = ctx_with_bridge();
        let sink = ChannelConnector.sink(&schema_config(), &ctx).unwrap();
        let src = ChannelConnector.source(&schema_config(), &ctx).unwrap();
        assert_eq!(src.schema().fields().len(), 1);
        assert_eq!(sink.schema().fields().len(), 1);
    }

    #[test]
    fn test_channel_source_missing_field_returns_error() {
        let ctx = ctx_with_bridge();
        let mut config = ConfigMap::new();
        config.insert(
            "name".to_string(),
            ConfigValue::String("bridge".to_string()),
        );
        let err = ChannelConnector
            .source(&ConfigValue::Object(config), &ctx)
            .err()
            .expect("should return error");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'field'"), "got: {err}");
    }

    #[test]
    fn test_channel_source_missing_name_returns_error() {
        let ctx = ctx_with_bridge();
        let raw = from_kdl_str("field \"id\" type=\"int64\" nullable=#false\n").unwrap();
        let err = ChannelConnector
            .source(&raw, &ctx)
            .err()
            .expect("should return error");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("`name`"));
    }

    #[test]
    fn test_channel_sink_missing_name_returns_error() {
        let ctx = ctx_with_bridge();
        let raw = from_kdl_str("field \"id\" type=\"int64\" nullable=#false\n").unwrap();
        let err = ChannelConnector
            .sink(&raw, &ctx)
            .err()
            .expect("should return error");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("`name`"));
    }

    #[test]
    fn test_channel_source_without_a_registered_bridge_returns_error() {
        let err = ChannelConnector
            .source(&schema_config(), &ConnectorContext::new(None))
            .err()
            .expect("should return error");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("channel bridge"));
    }

    #[test]
    fn test_channel_sink_without_a_registered_bridge_returns_error() {
        let err = ChannelConnector
            .sink(&schema_config(), &ConnectorContext::new(None))
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
            "name \"bridge\"\nbuffer 0\nfield \"id\" type=\"int64\" nullable=#false\n",
        )
        .unwrap();

        let err = ChannelConnector
            .source(&raw, &ctx)
            .err()
            .expect("a zero buffer must be refused");
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("'buffer' must be at least 1"),
            "got: {err}"
        );

        let err = ChannelConnector
            .sink(&raw, &ctx)
            .err()
            .expect("a zero buffer must be refused");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'buffer' must be at least 1"));
    }

    #[test]
    fn test_a_misspelled_key_is_named() {
        let ctx = ctx_with_bridge();
        let raw =
            from_kdl_str("name \"bridge\"\nbufer 4\nfield \"id\" type=\"int64\" nullable=#false\n")
                .unwrap();
        let err = ChannelConnector
            .source(&raw, &ctx)
            .err()
            .expect("an unknown key must be refused");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("bufer"), "got: {err}");
    }
}
