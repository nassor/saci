//! The TCP [`Connector`].
//!
//! One `connector="tcp"` names the listening half in a `source` node and the
//! connecting half in a `sink` node.
//!
//! [`TcpSource`] is a live source: it never reaches EOF, so it only works
//! under standalone stream mode (`run "stream"`). The service config
//! validator rejects `connector="tcp"` on a `source` node in any other mode.
//! That rule reads source nodes only, so a `tcp` sink runs in every mode.
//!
//! Producers write a `u32` big-endian length plus one payload per frame, and
//! [`TcpSink`] writes the same. What the payload bytes are is the codec's
//! business: the node's `codec` key names a declared `codec` node,
//! and that node's format decides how each frame encodes or decodes. There is
//! no default.

use crate::connector::schema::deserialize_config;
use crate::connector::{
    ConfigValue, Connector, ConnectorContext, Rebuild, Role, parse_fields, schema_of,
};
use crate::error::SaciResult;
use crate::io::{Sink, Source};

use crate::connector::tcp::{TcpSink, TcpSinkConfig, TcpSource, TcpSourceConfig};

/// The TCP connector: a `tcp` node of either role.
///
/// The source `config` table is [`TcpSourceConfig`]:
/// - `bind` (string, required): listen address, e.g. `"0.0.0.0:9500"`.
/// - `buffer` (usize, optional, default `64`): queued batch capacity.
/// - `max_frame_bytes` (usize, optional, default `8388608`): per-frame cap.
///
/// The sink `config` table is [`TcpSinkConfig`]:
/// - `connect` (string, required): peer address to dial, e.g.
///   `"collector.internal:9500"`.
///
/// Beside it, `field` entries (at least one, required) declare the Arrow
/// schema of either role.
///
/// Each frame's payload is decoded, and each batch encoded, by the codec the
/// host bound to this node from its `codec` key, reached through the
/// [`ConnectorContext`] rather than read out of `config`. The sink's address
/// is resolved at build and dialled on the first batch, so a peer that is
/// down fails at serve time, not at validate time.
pub struct TcpConnector;

impl Connector for TcpConnector {
    fn name(&self) -> &'static str {
        super::NAME
    }

    fn source(&self, config: &ConfigValue, ctx: &ConnectorContext) -> SaciResult<Box<dyn Source>> {
        let cfg: TcpSourceConfig = deserialize_config(config, "TcpSource")?;
        let schema = schema_of(&parse_fields(config, "tcp")?)?;
        let codec = ctx.codec("tcp")?;

        Ok(Box::new(TcpSource::new(cfg, schema, codec)?))
    }

    fn sink(&self, config: &ConfigValue, ctx: &ConnectorContext) -> SaciResult<Box<dyn Sink>> {
        let cfg: TcpSinkConfig = deserialize_config(config, "TcpSink")?;
        let schema = schema_of(&parse_fields(config, "tcp")?)?;
        let codec = ctx.codec("tcp")?;

        Ok(Box::new(TcpSink::new(cfg, schema, codec)?))
    }

    /// A source's listener is dropped before a rebuild, so its bind address
    /// is free again; frames decoded but not yet delivered are lost and
    /// producers must redial, which is what a service restart costs too. One
    /// socket serves a sink's whole life and a fresh instance dials again on
    /// its first write; a rebuilt sink drops whatever the failed one still
    /// buffered, along with its socket.
    fn rebuild(&self, _role: Role, _config: &ConfigValue) -> Rebuild {
        Rebuild::Yes
    }

    /// A listener never reaches end of input.
    fn live(&self, _config: &ConfigValue) -> bool {
        true
    }

    fn detail_keys(&self, _role: Role) -> &'static [&'static str] {
        &["bind", "connect"]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::arrow_ipc::ArrowIpcCodecFactory;
    use crate::codec::{Codec, CodecFactory};
    use crate::connector::{ConfigMap, from_kdl_str};
    use std::sync::Arc;

    /// A built codec, the way the host hands one to a connector.
    fn codec() -> Arc<dyn Codec> {
        ArrowIpcCodecFactory
            .build(&ConfigValue::Object(ConfigMap::new()))
            .expect("build arrow-ipc codec")
    }

    fn config(extra: &str) -> ConfigValue {
        let raw = format!(
            r#"
bind "127.0.0.1:0"
{extra}

field "v" type="int64" nullable=#false
"#
        );
        from_kdl_str(&raw).expect("parse test config")
    }

    #[test]
    fn builds_with_defaults() {
        let source = TcpConnector
            .source(&config(""), &ConnectorContext::new(Some(codec())))
            .expect("build");
        assert_eq!(source.schema().fields().len(), 1);
    }

    #[test]
    fn missing_bind_is_a_configuration_error() {
        let cfg = from_kdl_str(
            r#"
field "v" type="int64"
"#,
        )
        .unwrap();
        let err = TcpConnector
            .source(&cfg, &ConnectorContext::new(None))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("`bind`"), "got: {err}");
    }

    #[test]
    fn a_missing_codec_is_a_configuration_error() {
        let err = TcpConnector
            .source(&config(""), &ConnectorContext::new(None))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.message().contains("'codec'"), "got: {err}");
    }

    #[test]
    fn missing_field_is_a_configuration_error() {
        let cfg = from_kdl_str("bind \"127.0.0.1:0\"\n").unwrap();
        let err = TcpConnector
            .source(&cfg, &ConnectorContext::new(None))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'field'"), "got: {err}");
    }

    fn sink_config() -> ConfigValue {
        from_kdl_str(
            r#"
connect "127.0.0.1:9500"

field "v" type="int64" nullable=#false
"#,
        )
        .expect("parse test config")
    }

    #[test]
    fn the_sink_builds_from_connect_and_field() {
        let sink = TcpConnector
            .sink(&sink_config(), &ConnectorContext::new(Some(codec())))
            .expect("build");
        assert_eq!(sink.schema().fields().len(), 1);
    }

    /// One config `connector` names both halves; the roles keep them apart.
    #[test]
    fn the_connector_is_named_tcp_and_serves_both_roles() {
        assert_eq!(TcpConnector.name(), "tcp");
        assert_eq!(TcpConnector.roles(), &[Role::Source, Role::Sink]);
        assert_eq!(TcpConnector.detail_keys(Role::Source), &["bind", "connect"]);
        assert_eq!(TcpConnector.detail_keys(Role::Sink), &["bind", "connect"]);
    }

    #[test]
    fn a_tcp_source_is_live_and_both_roles_rebuild() {
        assert!(TcpConnector.live(&config("")));
        assert_eq!(
            TcpConnector.rebuild(Role::Source, &config("")),
            Rebuild::Yes
        );
        assert_eq!(
            TcpConnector.rebuild(Role::Sink, &sink_config()),
            Rebuild::Yes
        );
    }

    #[test]
    fn sink_missing_connect_is_a_configuration_error() {
        let cfg = from_kdl_str(
            r#"
field "v" type="int64"
"#,
        )
        .unwrap();
        let err = TcpConnector
            .sink(&cfg, &ConnectorContext::new(None))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("`connect`"), "got: {err}");
    }

    #[test]
    fn sink_missing_field_is_a_configuration_error() {
        let cfg = from_kdl_str("connect \"127.0.0.1:9500\"\n").unwrap();
        let err = TcpConnector
            .sink(&cfg, &ConnectorContext::new(None))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'field'"), "got: {err}");
    }

    #[test]
    fn a_sink_missing_its_codec_is_a_configuration_error() {
        let err = TcpConnector
            .sink(&sink_config(), &ConnectorContext::new(None))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.message().contains("'codec'"), "got: {err}");
    }

    #[test]
    fn a_misspelled_key_is_named() {
        let err = TcpConnector
            .source(
                &config("max_frame_byts 1024"),
                &ConnectorContext::new(Some(codec())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("max_frame_byts"), "got: {err}");
    }

    #[test]
    fn a_zero_buffer_is_refused_not_clamped() {
        let err = TcpConnector
            .source(&config("buffer 0"), &ConnectorContext::new(Some(codec())))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(
            err.to_string().contains("'buffer' must be at least 1"),
            "got: {err}"
        );
    }
}
