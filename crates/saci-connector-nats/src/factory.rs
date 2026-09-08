//! The NATS source and sink factories.
//!
//! Both deserialise the whole `config` node with serde rather than reading
//! `ConfigValue` keys by hand: the connector's configuration reaches every
//! connection, auth, TLS, consumer, stream and publish knob `async-nats`
//! exposes, and `#[serde(deny_unknown_fields)]` on every config struct is what
//! turns a misspelled key into a startup error instead of a silently ignored
//! setting.
//!
//! [`NatsSource::new`] and [`NatsSink::new`] are synchronous and open no
//! connection, so `saci-service validate` stays broker-free; `serve` fails on the
//! first `next_batch`/`write_batch` if the servers are unreachable.

use serde::Deserialize;

use saci_connector::{
    ConfigValue, ConnectorContext, SinkFactory, SourceFactory, parse_schema_fields,
};
use saci_core::error::SaciError;
use saci_core::io::{sink::Sink, source::Source};

use crate::{NatsSink, NatsSinkConfig, NatsSource, NatsSourceConfig};

/// Factory for [`NatsSource`].
///
/// The `config` node is [`NatsSourceConfig`]: a `connection` node, a `mode`
/// node selecting core NATS or JetStream, and `schema_fields` entries
/// declaring the Arrow schema. The payload codec is whatever transformer the
/// node's `transformer` key names.
pub struct NatsSourceFactory;

impl SourceFactory for NatsSourceFactory {
    fn type_name(&self) -> &'static str {
        "NatsSource"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        let cfg = NatsSourceConfig::deserialize(config.clone())
            .map_err(|e| SaciError::configuration(format!("NatsSource config: {e}")))?;
        let schema = parse_schema_fields(config, "NatsSource")?;
        let transformer = ctx.transformer("NatsSource")?;
        Ok(Box::new(NatsSource::new(cfg, schema, transformer)?))
    }

    /// JetStream re-delivers whatever a dropped instance pulled and never
    /// acked; core NATS re-delivers nothing, healed or not.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Ok(())
    }
}

/// Factory for [`NatsSink`].
///
/// The `config` node is [`NatsSinkConfig`]: a `connection` node, a `mode`
/// node selecting core NATS or JetStream, and `schema_fields` entries
/// declaring the Arrow schema. The payload codec is whatever transformer the
/// node's `transformer` key names.
pub struct NatsSinkFactory;

impl SinkFactory for NatsSinkFactory {
    fn type_name(&self) -> &'static str {
        "NatsSink"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError> {
        let cfg = NatsSinkConfig::deserialize(config.clone())
            .map_err(|e| SaciError::configuration(format!("NatsSink config: {e}")))?;
        let schema = parse_schema_fields(config, "NatsSink")?;
        let transformer = ctx.transformer("NatsSink")?;
        Ok(Box::new(NatsSink::new(cfg, schema, transformer)?))
    }

    /// A publish is awaited before `write_batch` returns, so a failed write
    /// leaves nothing buffered for a rebuild to discard.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use saci_connector::from_kdl_str;
    use saci_transformer::Transformer;
    use saci_transformer_ndjson::NdjsonTransformer;

    const SOURCE: &str = r#"
connection {
    servers "nats://localhost:4222"
}

mode kind="core" subject="orders"

schema_fields "id" type="int64" nullable=#false
"#;

    const SINK: &str = r#"
connection {
    servers "nats://localhost:4222"
}

mode kind="core" subject="orders-out"

schema_fields "id" type="int64" nullable=#false
"#;

    /// The context a host hands a node whose `transformer` key named a
    /// declared transformer.
    fn bound(transformer: Arc<dyn Transformer>) -> ConnectorContext {
        ConnectorContext::new(Some(transformer))
    }

    /// The context a host hands a node that declared no `transformer` key.
    fn unbound() -> ConnectorContext {
        ConnectorContext::new(None)
    }

    fn ndjson() -> Arc<dyn Transformer> {
        Arc::new(NdjsonTransformer::default())
    }

    #[test]
    fn type_names_match_the_config_type_key() {
        assert_eq!(NatsSourceFactory.type_name(), "NatsSource");
        assert_eq!(NatsSinkFactory.type_name(), "NatsSink");
    }

    #[test]
    fn a_config_node_missing_connection_is_a_configuration_error() {
        let raw = r#"
mode kind="core" subject="orders"
"#;
        let value = from_kdl_str(raw).expect("parse kdl");
        let Err(err) = NatsSourceFactory.build(&value, &bound(ndjson())) else {
            panic!("a missing connection node must fail");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("connection"), "got: {err}");
    }

    #[test]
    fn deserialisation_runs_before_schema_parsing() {
        // No `mode` and no `schema_fields`: the error must come from the missing
        // `mode` node, proving `NatsSourceConfig::deserialize` runs (and fails)
        // before `parse_schema_fields` is ever called.
        let value = from_kdl_str("connection {\n    servers \"nats://localhost:4222\"\n}\n")
            .expect("parse kdl");
        let Err(err) = NatsSourceFactory.build(&value, &bound(ndjson())) else {
            panic!("a missing mode node must fail");
        };
        assert!(err.message().contains("mode"), "got: {err}");
    }

    #[test]
    fn a_well_formed_source_config_builds() {
        let value = from_kdl_str(SOURCE).expect("parse kdl");
        NatsSourceFactory
            .build(&value, &bound(ndjson()))
            .expect("well-formed config must build");
    }

    #[test]
    fn a_well_formed_sink_config_builds() {
        let value = from_kdl_str(SINK).expect("parse kdl");
        NatsSinkFactory
            .build(&value, &bound(ndjson()))
            .expect("well-formed config must build");
    }

    #[test]
    fn a_node_with_no_transformer_is_a_configuration_error() {
        for (what, body) in [("NatsSource", SOURCE), ("NatsSink", SINK)] {
            let value = from_kdl_str(body).expect("parse kdl");
            let err = if what == "NatsSource" {
                NatsSourceFactory.build(&value, &unbound()).err()
            } else {
                NatsSinkFactory.build(&value, &unbound()).err()
            }
            .unwrap_or_else(|| panic!("{what} moves bytes, so it needs a transformer"));
            assert_eq!(err.category(), "configuration");
            assert!(err.message().starts_with(what), "got: {err}");
            assert!(err.message().contains("'transformer'"), "got: {err}");
        }
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
    fn a_transformer_with_no_message_codec_is_rejected() {
        let value = from_kdl_str(SINK).expect("parse kdl");
        let stream_only: Arc<dyn Transformer> = Arc::new(StreamOnly);

        let Err(err) = NatsSinkFactory.build(&value, &bound(stream_only)) else {
            panic!("a format with no message surface must be refused");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("has no message codec"), "got: {err}");
        assert!(err.message().contains("'stream-only'"), "got: {err}");
    }

    #[test]
    fn a_subject_field_against_a_batch_per_message_format_is_rejected() {
        let raw = r#"
connection {
    servers "nats://localhost:4222"
}

mode kind="core" subject="orders-out" subject_field="id"

schema_fields "id" type="int64" nullable=#false
"#;
        let value = from_kdl_str(raw).expect("parse kdl");
        let arrow_ipc: Arc<dyn Transformer> =
            Arc::new(saci_transformer_arrow_ipc::ArrowIpcTransformer::new());
        let Err(err) = NatsSinkFactory.build(&value, &bound(arrow_ipc)) else {
            panic!("arrow-ipc emits one message per batch, so there is no row to route");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'mode.subject_field'"), "got: {err}");
    }
}
