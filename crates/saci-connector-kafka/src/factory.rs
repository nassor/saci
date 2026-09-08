//! The Kafka source and sink factories.
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

use saci_connector::{
    ConfigValue, ConnectorContext, SinkFactory, SourceFactory, parse_schema_fields,
};
use saci_core::error::SaciError;
use saci_core::io::{sink::Sink, source::Source};

use crate::{KafkaSink, KafkaSinkConfig, KafkaSource, KafkaSourceConfig};

/// Factory for [`KafkaSource`].
///
/// The `config` table is [`KafkaSourceConfig`]: `brokers`, `topic`, and the
/// `schema_fields` entries declaring the Arrow schema, plus the consumer and
/// provisioning knobs documented on the config type. The payload codec is the
/// transformer the node's `transformer` key names.
pub struct KafkaSourceFactory;

impl SourceFactory for KafkaSourceFactory {
    fn type_name(&self) -> &'static str {
        "KafkaSource"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        let cfg = KafkaSourceConfig::deserialize(config.clone())
            .map_err(|e| SaciError::configuration(format!("KafkaSource config: {e}")))?;
        let schema = parse_schema_fields(config, "KafkaSource")?;
        let transformer = ctx.transformer("KafkaSource")?;
        Ok(Box::new(KafkaSource::new(cfg, schema, transformer)?))
    }

    /// A fresh consumer resumes from the committed group offset; the
    /// uncommitted batch is re-delivered, the at-least-once contract this
    /// source already documents.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Ok(())
    }
}

/// Factory for [`KafkaSink`].
///
/// The `config` table is [`KafkaSinkConfig`]: `brokers`, `topic`, and the
/// `schema_fields` entries declaring the Arrow schema, plus the producer and
/// provisioning knobs documented on the config type. The payload codec is the
/// transformer the node's `transformer` key names.
pub struct KafkaSinkFactory;

impl SinkFactory for KafkaSinkFactory {
    fn type_name(&self) -> &'static str {
        "KafkaSink"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError> {
        let cfg = KafkaSinkConfig::deserialize(config.clone())
            .map_err(|e| SaciError::configuration(format!("KafkaSink config: {e}")))?;
        let schema = parse_schema_fields(config, "KafkaSink")?;
        let transformer = ctx.transformer("KafkaSink")?;
        Ok(Box::new(KafkaSink::new(cfg, schema, transformer)?))
    }

    /// `write_batch` awaits every delivery and then flushes, so a failed
    /// write leaves nothing queued for a rebuild to discard.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use saci_connector::{ConfigMap, from_kdl_str};
    use saci_transformer::{Transformer, TransformerFactory};
    use saci_transformer_ndjson::NdjsonTransformerFactory;

    const SOURCE: &str = r#"
brokers "localhost:9092"
topic "orders"

schema_fields "id" type="int64" nullable=#false
"#;

    const SINK: &str = r#"
brokers "localhost:9092"
topic "orders-out"

schema_fields "id" type="int64" nullable=#false
"#;

    /// The host resolves a declared `transformer` node once per workflow
    /// build, so a test builds the instance itself and hands it over.
    fn context(factory: impl TransformerFactory) -> ConnectorContext {
        let transformer: Arc<dyn Transformer> = factory
            .build(&ConfigValue::Object(ConfigMap::new()))
            .expect("a transformer factory must build from empty options");
        ConnectorContext::new(Some(transformer))
    }

    #[test]
    fn type_names_match_the_config_type_property() {
        assert_eq!(KafkaSourceFactory.type_name(), "KafkaSource");
        assert_eq!(KafkaSinkFactory.type_name(), "KafkaSink");
    }

    #[test]
    fn a_config_table_missing_brokers_is_a_configuration_error() {
        let raw = r#"
topic "orders"

schema_fields "id" type="int64"
"#;
        let value = from_kdl_str(raw).expect("parse kdl");
        let ctx = context(NdjsonTransformerFactory);
        let Err(err) = KafkaSourceFactory.build(&value, &ctx) else {
            panic!("missing brokers must fail");
        };
        assert_eq!(err.category(), "configuration");
    }

    #[test]
    fn deserialisation_runs_before_schema_parsing() {
        // No `brokers` and no `schema_fields`: the error must come from the
        // missing `brokers` field, proving `KafkaSourceConfig::deserialize`
        // runs (and fails) before `parse_schema_fields` is ever called.
        let value = from_kdl_str(r#"topic "orders""#).expect("parse kdl");
        let ctx = context(NdjsonTransformerFactory);
        let Err(err) = KafkaSourceFactory.build(&value, &ctx) else {
            panic!("missing brokers must fail");
        };
        assert!(err.message().contains("brokers"));
    }

    #[test]
    fn a_source_with_no_bound_transformer_is_a_configuration_error() {
        let value = from_kdl_str(SOURCE).expect("parse kdl");
        let Err(err) = KafkaSourceFactory.build(&value, &ConnectorContext::new(None)) else {
            panic!("KafkaSource moves bytes, so it needs a transformer");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'transformer'"), "got: {err}");
    }

    // `StreamConsumer::create` spawns a background wake-loop task at
    // construction time, so building a real `KafkaSource` needs an active
    // Tokio runtime even though this test opens no connection.
    #[tokio::test]
    async fn a_well_formed_source_config_builds() {
        let value = from_kdl_str(SOURCE).expect("parse kdl");
        KafkaSourceFactory
            .build(&value, &context(NdjsonTransformerFactory))
            .expect("well-formed config must build");
    }

    #[test]
    fn a_key_field_against_a_batch_per_message_format_is_rejected() {
        let value =
            from_kdl_str(&SINK.replace("topic ", "key_field \"id\"\ntopic ")).expect("parse kdl");
        let ctx = context(saci_transformer_arrow_ipc::ArrowIpcTransformerFactory);

        let Err(err) = KafkaSinkFactory.build(&value, &ctx) else {
            panic!("arrow-ipc emits one message per batch, so there is no row to key");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'key_field'"), "got: {err}");
    }

    /// A stream-only format: every message method keeps the [`Transformer`]
    /// contract's `unsupported` default. Every format SACI registers implements
    /// the message surface, so the gate is exercised against a stand-in rather
    /// than a shipped format.
    struct StreamOnly;

    impl Transformer for StreamOnly {
        fn format(&self) -> &'static str {
            "stream-only"
        }
    }

    #[test]
    fn a_format_with_no_message_codec_is_rejected() {
        let value = from_kdl_str(SINK).expect("parse kdl");
        let ctx = ConnectorContext::new(Some(Arc::new(StreamOnly) as Arc<dyn Transformer>));

        let Err(err) = KafkaSinkFactory.build(&value, &ctx) else {
            panic!("a format with no message surface must be refused");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'stream-only'"), "got: {err}");
        assert!(err.message().contains("no message codec"), "got: {err}");
    }

    #[test]
    fn a_well_formed_sink_config_builds() {
        let value = from_kdl_str(SINK).expect("parse kdl");
        KafkaSinkFactory
            .build(&value, &context(NdjsonTransformerFactory))
            .expect("well-formed config must build");
    }

    #[tokio::test]
    async fn a_compacted_source_config_reaches_the_source() {
        // `key_field` naming a declared column is checked by `KafkaSource::new`,
        // so a build that succeeds proves both keys crossed the factory.
        let value =
            from_kdl_str(&SOURCE.replace("topic ", "compacted #true\nkey_field \"id\"\ntopic "))
                .expect("parse kdl");
        KafkaSourceFactory
            .build(&value, &context(NdjsonTransformerFactory))
            .expect("a compacted config must build");
    }

    #[tokio::test]
    async fn a_compacted_source_keyed_on_an_undeclared_column_is_rejected() {
        let value = from_kdl_str(
            &SOURCE.replace("topic ", "compacted #true\nkey_field \"absent\"\ntopic "),
        )
        .expect("parse kdl");
        let Err(err) = KafkaSourceFactory.build(&value, &context(NdjsonTransformerFactory)) else {
            panic!("'absent' is not one of the declared schema_fields");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("absent"), "got: {err}");
    }

    #[test]
    fn a_tombstone_sink_config_reaches_the_sink() {
        let value =
            from_kdl_str(&SINK.replace("topic ", "tombstones #true\nkey_field \"id\"\ntopic "))
                .expect("parse kdl");
        KafkaSinkFactory
            .build(&value, &context(NdjsonTransformerFactory))
            .expect("a tombstone-enabled config must build");
    }
}
