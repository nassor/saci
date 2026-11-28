//! The UDP connector.
//!
//! One [`UdpConnector`] named `udp` builds the receiving half for a `source`
//! node and the sending half for a `sink` node.
//!
//! [`UdpSource`] is a live source: it never reaches EOF, so it only works
//! under standalone stream mode (`run "stream"`). The service config
//! validator rejects `connector="udp"` on a `source` node in any other mode.
//! That rule reads source nodes only, so a `udp` sink runs in every mode.
//!
//! Producers write one payload per datagram, and [`UdpSink`] writes the same.
//! What the payload bytes are is the codec's business: the node's
//! `codec` key names a declared `codec` node, and that node's
//! format decides how each datagram encodes or decodes. There is no default.

use crate::connector::schema::deserialize_config;
use crate::connector::{
    ConfigValue, Connector, ConnectorContext, Rebuild, Role, parse_fields, schema_of,
};
use crate::error::SaciError;
use crate::io::{Sink, Source};

use crate::connector::udp::{UdpSink, UdpSinkConfig, UdpSource, UdpSourceConfig};

/// The `udp` connector: [`UdpSource`] and [`UdpSink`].
///
/// # Source config
///
/// The `config` table is [`UdpSourceConfig`]:
/// - `bind` (string, required): receive address, e.g. `"0.0.0.0:9700"`.
/// - `buffer` (usize, optional, default `64`): queued batch capacity.
/// - `multicast_group` (string, optional): a multicast address to join, of the
///   same IP family as `bind`.
///
/// Beside it, `field` entries (at least one, required) declare the Arrow
/// schema.
///
/// Each datagram is decoded by the codec the host bound to this node
/// from its `codec` key, reached through the [`ConnectorContext`] rather
/// than read out of `config`.
///
/// # Sink config
///
/// The `config` table is [`UdpSinkConfig`]:
/// - `connect` (string or list of strings, required): peer addresses, e.g.
///   `"collector.internal:9701"`. Every message goes to every peer.
/// - `broadcast` (bool, optional, default `#false`): enable `SO_BROADCAST`.
/// - `max_datagram_bytes` (usize, optional, default `65507`): per-message cap.
/// - `probe` (node, optional): `kind` (`"echo"` or `"icmp"`, default
///   `"echo"`), `interval_ms` (default `5000`), `timeout_ms` (default `1000`),
///   `on_failure` (`"refuse"` or `"warn"`, default `"refuse"`).
///
/// Beside it, `field` entries (at least one, required) declare the Arrow
/// schema.
///
/// Each batch is encoded by the codec the host bound to this node from
/// its `codec` key, reached through the [`ConnectorContext`] rather than
/// read out of `config`. The addresses are resolved here and the sockets open
/// on the first batch, so a peer that is down fails at serve time, not at
/// validate time.
pub struct UdpConnector;

impl Connector for UdpConnector {
    fn name(&self) -> &'static str {
        super::NAME
    }

    fn source(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        let cfg: UdpSourceConfig = deserialize_config(config, "UdpSource")?;
        let schema = schema_of(&parse_fields(config, "udp")?)?;
        let codec = ctx.codec("udp")?;

        let mut src = UdpSource::new(cfg, schema, codec)?;
        src.set_skip_corrupt(ctx.skip_corrupt());
        Ok(Box::new(src))
    }

    fn sink(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError> {
        let cfg: UdpSinkConfig = deserialize_config(config, "UdpSink")?;
        let schema = schema_of(&parse_fields(config, "udp")?)?;
        let codec = ctx.codec("udp")?;

        Ok(Box::new(UdpSink::new(cfg, schema, codec)?))
    }

    /// A source drops its socket and multicast membership before a rebuild,
    /// so the bind address is free again. Datagrams decoded but not yet
    /// delivered are lost, and so is everything that arrives during the gap,
    /// which is what a service restart costs too.
    ///
    /// A sink buffers nothing, so a rebuild loses no datagram it had
    /// accepted. A fresh instance re-resolves every `connect` address and
    /// opens fresh sockets, which is the whole of what reconnecting means for
    /// UDP.
    fn rebuild(&self, _role: Role, _config: &ConfigValue) -> Rebuild {
        Rebuild::Yes
    }

    /// A bound socket waits for datagrams for as long as it is open.
    fn live(&self, _config: &ConfigValue) -> bool {
        true
    }

    fn detail_keys(&self, _role: Role) -> &'static [&'static str] {
        &[
            "bind",
            "connect",
            "multicast_group",
            "broadcast",
            "probe.kind",
            "probe.interval_ms",
            "probe.timeout_ms",
            "probe.on_failure",
        ]
    }
}

#[cfg(all(test, feature = "codec-arrow-ipc"))]
mod tests {
    use super::*;
    use crate::codec::arrow_ipc::ArrowIpcCodecFactory;
    use crate::codec::{Codec, CodecFactory};
    use crate::connector::udp::{OnFailure, ProbeConfig, ProbeKind};
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
        let source = UdpConnector
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
        let err = UdpConnector
            .source(&cfg, &ConnectorContext::new(None))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("`bind`"), "got: {err}");
    }

    #[test]
    fn a_missing_codec_is_a_configuration_error() {
        let err = UdpConnector
            .source(&config(""), &ConnectorContext::new(None))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.message().contains("'codec'"), "got: {err}");
    }

    #[test]
    fn missing_field_is_a_configuration_error() {
        let cfg = from_kdl_str("bind \"127.0.0.1:0\"\n").unwrap();
        let err = UdpConnector
            .source(&cfg, &ConnectorContext::new(None))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'field'"), "got: {err}");
    }

    #[test]
    fn a_bad_multicast_group_is_a_configuration_error() {
        let err = UdpConnector
            .source(
                &config("multicast_group \"not-an-address\""),
                &ConnectorContext::new(Some(codec())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("multicast_group"), "got: {err}");
    }

    fn sink_config(extra: &str) -> ConfigValue {
        let raw = format!(
            r#"
connect "127.0.0.1:9701"
{extra}

field "v" type="int64" nullable=#false
"#
        );
        from_kdl_str(&raw).expect("parse test config")
    }

    #[test]
    fn the_sink_builds_from_connect_and_field() {
        let sink = UdpConnector
            .sink(&sink_config(""), &ConnectorContext::new(Some(codec())))
            .expect("build");
        assert_eq!(sink.schema().fields().len(), 1);
    }

    /// One connector name serves both halves.
    #[test]
    fn one_connector_serves_both_roles_as_udp() {
        assert_eq!(UdpConnector.name(), "udp");
        assert_eq!(UdpConnector.roles(), &[Role::Source, Role::Sink]);
    }

    #[test]
    fn a_udp_source_is_live_and_both_roles_are_rebuildable() {
        assert!(UdpConnector.live(&config("")));
        assert_eq!(
            UdpConnector.rebuild(Role::Source, &config("")),
            Rebuild::Yes
        );
        assert_eq!(
            UdpConnector.rebuild(Role::Sink, &sink_config("")),
            Rebuild::Yes
        );
    }

    #[test]
    fn both_roles_show_the_same_detail_keys() {
        let keys = UdpConnector.detail_keys(Role::Source);
        assert_eq!(keys, UdpConnector.detail_keys(Role::Sink));
        assert!(keys.contains(&"bind"));
        assert!(keys.contains(&"connect"));
        assert!(keys.contains(&"probe.on_failure"));
    }

    #[test]
    fn sink_missing_connect_is_a_configuration_error() {
        let cfg = from_kdl_str(
            r#"
field "v" type="int64"
"#,
        )
        .unwrap();
        let err = UdpConnector
            .sink(&cfg, &ConnectorContext::new(None))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("`connect`"), "got: {err}");
    }

    #[test]
    fn sink_missing_field_is_a_configuration_error() {
        let cfg = from_kdl_str("connect \"127.0.0.1:9701\"\n").unwrap();
        let err = UdpConnector
            .sink(&cfg, &ConnectorContext::new(None))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'field'"), "got: {err}");
    }

    #[test]
    fn a_sink_missing_its_codec_is_a_configuration_error() {
        let err = UdpConnector
            .sink(&sink_config(""), &ConnectorContext::new(None))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.message().contains("'codec'"), "got: {err}");
    }

    #[test]
    fn a_connect_list_fans_out() {
        let cfg = from_kdl_str(
            r#"
connect "127.0.0.1:9701" "127.0.0.1:9702"

field "v" type="int64" nullable=#false
"#,
        )
        .expect("parse test config");
        let parsed: UdpSinkConfig = deserialize_config(&cfg, "UdpSink").expect("parse connect");
        assert_eq!(
            parsed.connect,
            vec!["127.0.0.1:9701".to_string(), "127.0.0.1:9702".to_string()]
        );
        UdpConnector
            .sink(&cfg, &ConnectorContext::new(Some(codec())))
            .expect("build");
    }

    /// A bare `probe` node parses to an empty object, which is what makes the
    /// line alone a complete declaration.
    #[test]
    fn a_bare_probe_node_enables_the_defaults() {
        let cfg = sink_config("probe");
        let parsed: UdpSinkConfig = deserialize_config(&cfg, "UdpSink").expect("parse probe");
        assert_eq!(
            parsed.probe.expect("a bare node still declares a probe"),
            ProbeConfig {
                kind: ProbeKind::Echo,
                interval_ms: 5000,
                timeout_ms: 1000,
                on_failure: OnFailure::Refuse,
            }
        );
        UdpConnector
            .sink(&cfg, &ConnectorContext::new(Some(codec())))
            .expect("build");
    }

    #[test]
    fn an_unknown_probe_kind_is_a_configuration_error() {
        let err = UdpConnector
            .sink(
                &sink_config("probe kind=\"ping\""),
                &ConnectorContext::new(Some(codec())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("kind"), "got: {err}");
    }

    #[test]
    fn an_unknown_on_failure_is_a_configuration_error() {
        let err = UdpConnector
            .sink(
                &sink_config("probe on_failure=\"ignore\""),
                &ConnectorContext::new(Some(codec())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("on_failure"), "got: {err}");
    }

    #[test]
    fn a_probe_with_broadcast_is_a_configuration_error() {
        let err = UdpConnector
            .sink(
                &sink_config("broadcast #true\nprobe"),
                &ConnectorContext::new(Some(codec())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'probe'"), "got: {err}");
    }

    #[test]
    fn a_max_datagram_bytes_that_is_not_an_integer_is_a_configuration_error() {
        let err = UdpConnector
            .sink(
                &sink_config("max_datagram_bytes \"lots\""),
                &ConnectorContext::new(Some(codec())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("max_datagram_bytes"), "got: {err}");
    }

    #[test]
    fn a_negative_max_datagram_bytes_is_a_configuration_error() {
        let err = UdpConnector
            .sink(
                &sink_config("max_datagram_bytes -1"),
                &ConnectorContext::new(Some(codec())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("max_datagram_bytes"), "got: {err}");
    }

    #[test]
    fn a_probe_that_is_not_a_node_is_a_configuration_error() {
        let err = UdpConnector
            .sink(
                &sink_config("probe \"yes\""),
                &ConnectorContext::new(Some(codec())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("probe"), "got: {err}");
    }

    #[test]
    fn a_connect_list_with_a_non_string_entry_is_a_configuration_error() {
        let cfg = from_kdl_str(
            r#"
connect "127.0.0.1:1" 2

field "v" type="int64" nullable=#false
"#,
        )
        .expect("parse test config");
        let err = UdpConnector
            .sink(&cfg, &ConnectorContext::new(Some(codec())))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("connect"), "got: {err}");
    }

    #[test]
    fn a_bare_connect_is_a_configuration_error() {
        let cfg = from_kdl_str(
            r#"
connect

field "v" type="int64" nullable=#false
"#,
        )
        .expect("parse test config");
        let err = UdpConnector
            .sink(&cfg, &ConnectorContext::new(Some(codec())))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("connect"), "got: {err}");
    }

    #[test]
    fn a_probe_with_a_zero_interval_is_a_configuration_error() {
        let err = UdpConnector
            .sink(
                &sink_config("probe interval_ms=0"),
                &ConnectorContext::new(Some(codec())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("interval_ms"), "got: {err}");
    }

    #[test]
    fn a_group_of_the_other_family_is_a_configuration_error() {
        let err = UdpConnector
            .source(
                &config("multicast_group \"ff02::1\""),
                &ConnectorContext::new(Some(codec())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("same IP family"), "got: {err}");
    }

    #[test]
    fn a_zero_buffer_is_refused_not_clamped() {
        let err = UdpConnector
            .source(&config("buffer 0"), &ConnectorContext::new(Some(codec())))
            .err()
            .expect("a zero buffer must be refused");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(
            err.to_string().contains("'buffer' must be at least 1"),
            "got: {err}"
        );
    }

    #[test]
    fn a_misspelled_sink_key_is_named() {
        let err = UdpConnector
            .sink(
                &sink_config("max_datagram_byts 512"),
                &ConnectorContext::new(Some(codec())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("max_datagram_byts"), "got: {err}");
    }

    #[test]
    fn a_probe_kind_of_icmp_builds() {
        let cfg = sink_config("probe kind=\"icmp\"");
        let parsed: UdpSinkConfig = deserialize_config(&cfg, "UdpSink").expect("parse probe");
        let settings = parsed.probe.expect("a probe node declares a probe");
        assert_eq!(settings.kind, ProbeKind::Icmp);
        UdpConnector
            .sink(&cfg, &ConnectorContext::new(Some(codec())))
            .expect("build");
    }

    #[test]
    fn an_on_failure_of_warn_builds() {
        let cfg = sink_config("probe on_failure=\"warn\"");
        let parsed: UdpSinkConfig = deserialize_config(&cfg, "UdpSink").expect("parse probe");
        let settings = parsed.probe.expect("a probe node declares a probe");
        assert_eq!(settings.on_failure, OnFailure::Warn);
        UdpConnector
            .sink(&cfg, &ConnectorContext::new(Some(codec())))
            .expect("build");
    }

    #[test]
    fn a_non_string_multicast_group_is_a_configuration_error() {
        let err = UdpConnector
            .source(
                &config("multicast_group 239"),
                &ConnectorContext::new(Some(codec())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("multicast_group"), "got: {err}");
    }

    #[test]
    fn a_non_boolean_broadcast_is_a_configuration_error() {
        let err = UdpConnector
            .sink(
                &sink_config("broadcast \"true\""),
                &ConnectorContext::new(Some(codec())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("broadcast"), "got: {err}");
    }

    #[test]
    fn a_negative_probe_interval_is_a_configuration_error() {
        let err = UdpConnector
            .sink(
                &sink_config("probe interval_ms=-5"),
                &ConnectorContext::new(Some(codec())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("interval_ms"), "got: {err}");
    }

    #[test]
    fn a_non_integer_probe_timeout_is_a_configuration_error() {
        let err = UdpConnector
            .sink(
                &sink_config("probe timeout_ms=\"1s\""),
                &ConnectorContext::new(Some(codec())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("timeout_ms"), "got: {err}");
    }

    #[test]
    fn a_non_string_probe_kind_is_a_configuration_error() {
        let err = UdpConnector
            .sink(
                &sink_config("probe kind=5"),
                &ConnectorContext::new(Some(codec())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("kind"), "got: {err}");
    }

    #[test]
    fn a_non_string_on_failure_is_a_configuration_error() {
        let err = UdpConnector
            .sink(
                &sink_config("probe on_failure=1"),
                &ConnectorContext::new(Some(codec())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("on_failure"), "got: {err}");
    }
}
