//! The Kafka connector.
//!
//! Both deserialise the whole `config` table with serde rather than reading
//! [`ConfigValue`] keys by hand: the connector's configuration reaches
//! every librdkafka property plus a handful of SACI-level keys, and
//! `#[serde(deny_unknown_fields)]` on every config struct is what turns a
//! misspelled key into a startup error instead of a silently ignored setting.
//!
//! `KafkaSource::new` and `KafkaSink::new` are synchronous and open no
//! connection, so `saci-service validate` stays broker-free; `serve` fails on
//! the first `next_batch`/`write_batch` if the broker is unreachable.

use serde::Deserialize;

use crate::connector::schema::without_fields;
use crate::connector::{
    ConfigValue, Connector, ConnectorContext, Rebuild, Role, parse_fields, schema_of,
};
use crate::error::SaciError;
use crate::io::{Sink, Source};

use crate::connector::kafka::{KafkaSink, KafkaSinkConfig, KafkaSource, KafkaSourceConfig};

/// The `kafka` connector: a consumer source and a producer sink.
///
/// The source's `config` table is [`KafkaSourceConfig`] and the sink's is
/// [`KafkaSinkConfig`]: `brokers`, `topic`, and the `field` entries declaring
/// the Arrow schema, plus the consumer or producer and provisioning knobs
/// documented on each config type. The payload codec is the codec the node's
/// `codec` key names.
pub struct KafkaConnector;

impl Connector for KafkaConnector {
    fn name(&self) -> &'static str {
        "kafka"
    }

    fn source(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        let cfg = KafkaSourceConfig::deserialize(without_fields(config))
            .map_err(|e| SaciError::configuration(format!("KafkaSource config: {e}")))?;
        let schema = schema_of(&parse_fields(config, "KafkaSource")?)?;
        let codec = ctx.codec("KafkaSource")?;
        Ok(Box::new(KafkaSource::new(cfg, schema, codec)?))
    }

    fn sink(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError> {
        let cfg = KafkaSinkConfig::deserialize(without_fields(config))
            .map_err(|e| SaciError::configuration(format!("KafkaSink config: {e}")))?;
        let schema = schema_of(&parse_fields(config, "KafkaSink")?)?;
        let codec = ctx.codec("KafkaSink")?;
        Ok(Box::new(KafkaSink::new(cfg, schema, codec)?))
    }

    /// A fresh consumer resumes from the committed group offset; the
    /// uncommitted batch is re-delivered, the at-least-once contract the
    /// source already documents. `write_batch` awaits every delivery and then
    /// flushes, so a failed write leaves nothing queued for a rebuild to
    /// discard.
    fn rebuild(&self, _role: Role, _config: &ConfigValue) -> Rebuild {
        Rebuild::Yes
    }

    /// A source is bounded only when it stops at the end of the topic or
    /// reads a compacted topic to its end.
    fn live(&self, config: &ConfigValue) -> bool {
        let flag = |key: &str| {
            config
                .get(key)
                .and_then(ConfigValue::as_bool)
                .unwrap_or(false)
        };
        !(flag("stop_at_end") || flag("compacted"))
    }

    fn detail_keys(&self, _role: Role) -> &'static [&'static str] {
        &["topic"]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use crate::codec::ndjson::NdjsonCodecFactory;
    use crate::codec::{Codec, CodecFactory};
    use crate::connector::{ConfigMap, from_kdl_str};

    const SOURCE: &str = r#"
brokers "localhost:9092"
topic "orders"

field "id" type="int64" nullable=#false
"#;

    const SINK: &str = r#"
brokers "localhost:9092"
topic "orders-out"

field "id" type="int64" nullable=#false
"#;

    /// The host resolves a declared `codec` node once per workflow
    /// build, so a test builds the instance itself and hands it over.
    fn context(factory: impl CodecFactory) -> ConnectorContext {
        let codec: Arc<dyn Codec> = factory
            .build(&ConfigValue::Object(ConfigMap::new()))
            .expect("a codec factory must build from empty options");
        ConnectorContext::new(Some(codec))
    }

    #[test]
    fn the_connector_declares_its_name_roles_and_detail_keys() {
        assert_eq!(KafkaConnector.name(), "kafka");
        assert_eq!(KafkaConnector.roles(), &[Role::Source, Role::Sink]);
        assert_eq!(KafkaConnector.detail_keys(Role::Source), &["topic"]);
        assert_eq!(KafkaConnector.detail_keys(Role::Sink), &["topic"]);
        let empty = ConfigValue::Object(ConfigMap::new());
        assert_eq!(KafkaConnector.rebuild(Role::Source, &empty), Rebuild::Yes);
        assert_eq!(KafkaConnector.rebuild(Role::Sink, &empty), Rebuild::Yes);
    }

    #[test]
    fn a_source_is_live_unless_it_stops_at_the_end_or_is_compacted() {
        let live = from_kdl_str(SOURCE).expect("parse kdl");
        assert!(KafkaConnector.live(&live));
        let bounded = from_kdl_str(&SOURCE.replace("topic ", "stop_at_end #true\ntopic "))
            .expect("parse kdl");
        assert!(!KafkaConnector.live(&bounded));
        let compacted =
            from_kdl_str(&SOURCE.replace("topic ", "compacted #true\ntopic ")).expect("parse kdl");
        assert!(!KafkaConnector.live(&compacted));
    }

    #[test]
    fn a_config_table_missing_brokers_is_a_configuration_error() {
        let raw = r#"
topic "orders"

field "id" type="int64"
"#;
        let value = from_kdl_str(raw).expect("parse kdl");
        let ctx = context(NdjsonCodecFactory);
        let Err(err) = KafkaConnector.source(&value, &ctx) else {
            panic!("missing brokers must fail");
        };
        assert_eq!(err.category(), "configuration");
    }

    #[test]
    fn deserialisation_runs_before_schema_parsing() {
        // No `brokers` and no `field`: the error must come from the
        // missing `brokers` field, proving `KafkaSourceConfig::deserialize`
        // runs (and fails) before `parse_fields` is ever called.
        let value = from_kdl_str(r#"topic "orders""#).expect("parse kdl");
        let ctx = context(NdjsonCodecFactory);
        let Err(err) = KafkaConnector.source(&value, &ctx) else {
            panic!("missing brokers must fail");
        };
        assert!(err.message().contains("brokers"));
    }

    #[test]
    fn a_source_with_no_bound_codec_is_a_configuration_error() {
        let value = from_kdl_str(SOURCE).expect("parse kdl");
        let Err(err) = KafkaConnector.source(&value, &ConnectorContext::new(None)) else {
            panic!("KafkaSource moves bytes, so it needs a codec");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'codec'"), "got: {err}");
    }

    // `StreamConsumer::create` spawns a background wake-loop task at
    // construction time, so building a real `KafkaSource` needs an active
    // Tokio runtime even though this test opens no connection.
    #[tokio::test]
    async fn a_well_formed_source_config_builds() {
        let value = from_kdl_str(SOURCE).expect("parse kdl");
        KafkaConnector
            .source(&value, &context(NdjsonCodecFactory))
            .expect("well-formed config must build");
    }

    #[cfg(feature = "codec-arrow-ipc")]
    #[test]
    fn a_key_field_against_a_batch_per_message_format_is_rejected() {
        let value =
            from_kdl_str(&SINK.replace("topic ", "key_field \"id\"\ntopic ")).expect("parse kdl");
        let ctx = context(crate::codec::arrow_ipc::ArrowIpcCodecFactory);

        let Err(err) = KafkaConnector.sink(&value, &ctx) else {
            panic!("arrow-ipc emits one message per batch, so there is no row to key");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'key_field'"), "got: {err}");
    }

    /// A stream-only format: every message method keeps the [`Codec`]
    /// contract's `unsupported` default. Every format SACI registers implements
    /// the message surface, so the gate is exercised against a stand-in rather
    /// than a shipped format.
    struct StreamOnly;

    impl Codec for StreamOnly {
        fn format(&self) -> &'static str {
            "stream-only"
        }
    }

    #[test]
    fn a_format_with_no_message_codec_is_rejected() {
        let value = from_kdl_str(SINK).expect("parse kdl");
        let ctx = ConnectorContext::new(Some(Arc::new(StreamOnly) as Arc<dyn Codec>));

        let Err(err) = KafkaConnector.sink(&value, &ctx) else {
            panic!("a format with no message surface must be refused");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'stream-only'"), "got: {err}");
        assert!(err.message().contains("no message codec"), "got: {err}");
    }

    #[test]
    fn a_well_formed_sink_config_builds() {
        let value = from_kdl_str(SINK).expect("parse kdl");
        KafkaConnector
            .sink(&value, &context(NdjsonCodecFactory))
            .expect("well-formed config must build");
    }

    #[tokio::test]
    async fn a_compacted_source_config_reaches_the_source() {
        // `key_field` naming a declared column is checked by `KafkaSource::new`,
        // so a build that succeeds proves both keys crossed the factory.
        let value =
            from_kdl_str(&SOURCE.replace("topic ", "compacted #true\nkey_field \"id\"\ntopic "))
                .expect("parse kdl");
        KafkaConnector
            .source(&value, &context(NdjsonCodecFactory))
            .expect("a compacted config must build");
    }

    #[tokio::test]
    async fn a_compacted_source_keyed_on_an_undeclared_column_is_rejected() {
        let value = from_kdl_str(
            &SOURCE.replace("topic ", "compacted #true\nkey_field \"absent\"\ntopic "),
        )
        .expect("parse kdl");
        let Err(err) = KafkaConnector.source(&value, &context(NdjsonCodecFactory)) else {
            panic!("'absent' is not one of the declared fields");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("absent"), "got: {err}");
    }

    #[test]
    fn a_tombstone_sink_config_reaches_the_sink() {
        let value =
            from_kdl_str(&SINK.replace("topic ", "tombstones #true\nkey_field \"id\"\ntopic "))
                .expect("parse kdl");
        KafkaConnector
            .sink(&value, &context(NdjsonCodecFactory))
            .expect("a tombstone-enabled config must build");
    }
}
