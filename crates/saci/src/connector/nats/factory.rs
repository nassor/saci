//! The NATS connector.
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

use crate::connector::schema::without_fields;
use crate::connector::{
    ConfigValue, Connector, ConnectorContext, Rebuild, Role, parse_fields, schema_of,
};
use crate::error::SaciError;
use crate::io::{Sink, Source};

use crate::connector::nats::{NatsSink, NatsSinkConfig, NatsSource, NatsSourceConfig};

/// The `nats` connector: a core NATS or JetStream source and sink.
///
/// The source's `config` node is [`NatsSourceConfig`] and the sink's is
/// [`NatsSinkConfig`]: a `connection` node, a `mode` node selecting core NATS
/// or JetStream, and `field` entries declaring the Arrow schema. The payload
/// codec is whatever codec the node's `codec` key names.
pub struct NatsConnector;

impl Connector for NatsConnector {
    fn name(&self) -> &'static str {
        super::NAME
    }

    fn source(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        let cfg = NatsSourceConfig::deserialize(without_fields(config))
            .map_err(|e| SaciError::configuration(format!("NatsSource config: {e}")))?;
        let schema = schema_of(&parse_fields(config, "NatsSource")?)?;
        let codec = ctx.codec("NatsSource")?;
        let mut src = NatsSource::new(cfg, schema, codec)?;
        src.set_skip_corrupt(ctx.skip_corrupt());
        Ok(Box::new(src))
    }

    fn sink(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError> {
        let cfg = NatsSinkConfig::deserialize(without_fields(config))
            .map_err(|e| SaciError::configuration(format!("NatsSink config: {e}")))?;
        let schema = schema_of(&parse_fields(config, "NatsSink")?)?;
        let codec = ctx.codec("NatsSink")?;
        Ok(Box::new(NatsSink::new(cfg, schema, codec)?))
    }

    /// JetStream re-delivers whatever a dropped instance pulled and never
    /// acked; core NATS re-delivers nothing, healed or not. A publish is
    /// awaited before `write_batch` returns, so a failed write leaves nothing
    /// buffered for a rebuild to discard.
    fn rebuild(&self, _role: Role, _config: &ConfigValue) -> Rebuild {
        Rebuild::Yes
    }

    /// A source is bounded only when it stops at the end of the stream.
    fn live(&self, config: &ConfigValue) -> bool {
        !config
            .get("stop_at_end")
            .and_then(ConfigValue::as_bool)
            .unwrap_or(false)
    }

    fn detail_keys(&self, _role: Role) -> &'static [&'static str] {
        &["mode.kind", "mode.stream", "mode.subject"]
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::codec::Codec;
    use crate::codec::ndjson::NdjsonCodec;
    use crate::connector::from_kdl_str;

    const SOURCE: &str = r#"
connection {
    servers "nats://localhost:4222"
}

mode kind="core" subject="orders"

field "id" type="int64" nullable=#false
"#;

    const SINK: &str = r#"
connection {
    servers "nats://localhost:4222"
}

mode kind="core" subject="orders-out"

field "id" type="int64" nullable=#false
"#;

    /// The context a host hands a node whose `codec` key named a
    /// declared codec.
    fn bound(codec: Arc<dyn Codec>) -> ConnectorContext {
        ConnectorContext::new(Some(codec))
    }

    /// The context a host hands a node that declared no `codec` key.
    fn unbound() -> ConnectorContext {
        ConnectorContext::new(None)
    }

    fn ndjson() -> Arc<dyn Codec> {
        Arc::new(NdjsonCodec::default())
    }

    #[test]
    fn the_connector_declares_its_name_roles_and_detail_keys() {
        assert_eq!(NatsConnector.name(), "nats");
        assert_eq!(NatsConnector.roles(), &[Role::Source, Role::Sink]);
        let keys: &[&str] = &["mode.kind", "mode.stream", "mode.subject"];
        assert_eq!(NatsConnector.detail_keys(Role::Source), keys);
        assert_eq!(NatsConnector.detail_keys(Role::Sink), keys);
        let value = from_kdl_str(SOURCE).expect("parse kdl");
        assert_eq!(NatsConnector.rebuild(Role::Source, &value), Rebuild::Yes);
        assert_eq!(NatsConnector.rebuild(Role::Sink, &value), Rebuild::Yes);
    }

    #[test]
    fn a_source_is_live_unless_it_stops_at_the_end() {
        let live = from_kdl_str(SOURCE).expect("parse kdl");
        assert!(NatsConnector.live(&live));
        let bounded = from_kdl_str(&format!("stop_at_end #true\n{SOURCE}")).expect("parse kdl");
        assert!(!NatsConnector.live(&bounded));
    }

    #[test]
    fn a_config_node_missing_connection_is_a_configuration_error() {
        let raw = r#"
mode kind="core" subject="orders"
"#;
        let value = from_kdl_str(raw).expect("parse kdl");
        let Err(err) = NatsConnector.source(&value, &bound(ndjson())) else {
            panic!("a missing connection node must fail");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("connection"), "got: {err}");
    }

    #[test]
    fn deserialisation_runs_before_schema_parsing() {
        // No `mode` and no `field`: the error must come from the missing
        // `mode` node, proving `NatsSourceConfig::deserialize` runs (and fails)
        // before `parse_fields` is ever called.
        let value = from_kdl_str("connection {\n    servers \"nats://localhost:4222\"\n}\n")
            .expect("parse kdl");
        let Err(err) = NatsConnector.source(&value, &bound(ndjson())) else {
            panic!("a missing mode node must fail");
        };
        assert!(err.message().contains("mode"), "got: {err}");
    }

    #[test]
    fn a_well_formed_source_config_builds() {
        let value = from_kdl_str(SOURCE).expect("parse kdl");
        NatsConnector
            .source(&value, &bound(ndjson()))
            .expect("well-formed config must build");
    }

    #[test]
    fn a_well_formed_sink_config_builds() {
        let value = from_kdl_str(SINK).expect("parse kdl");
        NatsConnector
            .sink(&value, &bound(ndjson()))
            .expect("well-formed config must build");
    }

    #[test]
    fn a_node_with_no_codec_is_a_configuration_error() {
        for (what, body) in [("NatsSource", SOURCE), ("NatsSink", SINK)] {
            let value = from_kdl_str(body).expect("parse kdl");
            let err = if what == "NatsSource" {
                NatsConnector.source(&value, &unbound()).err()
            } else {
                NatsConnector.sink(&value, &unbound()).err()
            }
            .unwrap_or_else(|| panic!("{what} moves bytes, so it needs a codec"));
            assert_eq!(err.category(), "configuration");
            assert!(err.message().starts_with(what), "got: {err}");
            assert!(err.message().contains("'codec'"), "got: {err}");
        }
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
    fn a_codec_with_no_message_codec_is_rejected() {
        let value = from_kdl_str(SINK).expect("parse kdl");
        let stream_only: Arc<dyn Codec> = Arc::new(StreamOnly);

        let Err(err) = NatsConnector.sink(&value, &bound(stream_only)) else {
            panic!("a format with no message surface must be refused");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("has no message codec"), "got: {err}");
        assert!(err.message().contains("'stream-only'"), "got: {err}");
    }

    #[cfg(feature = "codec-arrow-ipc")]
    #[test]
    fn a_subject_field_against_a_batch_per_message_format_is_rejected() {
        let raw = r#"
connection {
    servers "nats://localhost:4222"
}

mode kind="core" subject="orders-out" subject_field="id"

field "id" type="int64" nullable=#false
"#;
        let value = from_kdl_str(raw).expect("parse kdl");
        let arrow_ipc: Arc<dyn Codec> = Arc::new(crate::codec::arrow_ipc::ArrowIpcCodec::new());
        let Err(err) = NatsConnector.sink(&value, &bound(arrow_ipc)) else {
            panic!("arrow-ipc emits one message per batch, so there is no row to route");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'mode.subject_field'"), "got: {err}");
    }
}
