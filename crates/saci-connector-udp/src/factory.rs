//! The UDP source and sink factories.
//!
//! Both register as `type="udp"`: the registry keeps its source and sink maps
//! apart, so one string names the receiving half in a `source` node and the
//! sending half in a `sink` node.
//!
//! [`UdpSource`] is a live source: it never reaches EOF, so it only works
//! under standalone stream mode (`run_mode kind="stream"`). The service config
//! validator rejects `type="udp"` on a `source` node in any other mode. That
//! rule reads source nodes only, so a `udp` sink runs in every mode.
//!
//! Producers write one payload per datagram, and [`UdpSink`] writes the same.
//! What the payload bytes are is the transformer's business: the node's
//! `transformer` key names a declared `transformer` node, and that node's
//! format decides how each datagram encodes or decodes. There is no default.

use std::net::IpAddr;
use std::time::Duration;

use saci_connector::{
    ConfigValue, ConnectorContext, SinkFactory, SourceFactory, parse_schema_fields,
};
use saci_core::error::SaciError;
use saci_core::io::sink::Sink;
use saci_core::io::source::Source;

use crate::probe::{OnFailure, ProbeKind, ProbeSettings};
use crate::sink::MAX_DATAGRAM_BYTES;
use crate::{UdpSink, UdpSource};

/// Default number of decoded batches queued before the receive loop stops
/// reading the socket.
const DEFAULT_BUFFER: usize = 64;
/// Default gap between probe rounds.
const DEFAULT_PROBE_INTERVAL_MS: u64 = 5000;
/// Default wait for one probe round's answer.
const DEFAULT_PROBE_TIMEOUT_MS: u64 = 1000;

/// Factory for [`UdpSource`].
///
/// Config fields:
/// - `bind` (string, required): receive address, e.g. `"0.0.0.0:9700"`.
/// - `buffer` (usize, optional, default `64`): queued batch capacity.
/// - `multicast_group` (string, optional): a multicast address to join, of the
///   same IP family as `bind`.
/// - `schema_fields` (list, required): Arrow schema definition.
///
/// Each datagram is decoded by the transformer the host bound to this node
/// from its `transformer` key, reached through the [`ConnectorContext`] rather
/// than read out of `config`.
pub struct UdpSourceFactory;

impl SourceFactory for UdpSourceFactory {
    fn type_name(&self) -> &'static str {
        "udp"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        let bind = config.get("bind").and_then(|v| v.as_str()).ok_or_else(|| {
            SaciError::configuration("udp source config requires a 'bind' string")
        })?;

        let buffer = config
            .get("buffer")
            .and_then(|v| v.as_i64())
            .map(|v| v.max(1) as usize)
            .unwrap_or(DEFAULT_BUFFER);

        let multicast_group = match config.get("multicast_group") {
            None => None,
            Some(value) => {
                let raw = value.as_str().ok_or_else(|| {
                    SaciError::configuration("udp source 'multicast_group' must be a string")
                })?;
                Some(raw.parse::<IpAddr>().map_err(|_| {
                    SaciError::configuration(format!(
                        "udp source 'multicast_group' is not an IP address: '{raw}'"
                    ))
                })?)
            }
        };

        let schema = parse_schema_fields(config, "udp")?;
        let transformer = ctx.transformer("udp")?;

        Ok(Box::new(UdpSource::new(
            bind,
            multicast_group,
            schema,
            buffer,
            transformer,
        )?))
    }

    /// The socket and its multicast membership are dropped before a rebuild,
    /// so the bind address is free again. Datagrams decoded but not yet
    /// delivered are lost, and so is everything that arrives during the gap,
    /// which is what a service restart costs too.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Ok(())
    }
}

/// Factory for [`UdpSink`].
///
/// Config fields:
/// - `connect` (string or list of strings, required): peer addresses, e.g.
///   `"collector.internal:9701"`. Every message goes to every peer.
/// - `broadcast` (bool, optional, default `#false`): enable `SO_BROADCAST`.
/// - `max_datagram_bytes` (usize, optional, default `65507`): per-message cap.
/// - `probe` (node, optional): `kind` (`"echo"` or `"icmp"`, default
///   `"echo"`), `interval_ms` (default `5000`), `timeout_ms` (default `1000`),
///   `on_failure` (`"refuse"` or `"warn"`, default `"refuse"`).
/// - `schema_fields` (list, required): Arrow schema definition.
///
/// Each batch is encoded by the transformer the host bound to this node from
/// its `transformer` key, reached through the [`ConnectorContext`] rather than
/// read out of `config`. The addresses are resolved here and the sockets open
/// on the first batch, so a peer that is down fails at serve time, not at
/// validate time.
pub struct UdpSinkFactory;

impl SinkFactory for UdpSinkFactory {
    fn type_name(&self) -> &'static str {
        "udp"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError> {
        let connect = parse_connect(config)?;
        let broadcast = match config.get("broadcast") {
            None => false,
            Some(value) => value.as_bool().ok_or_else(|| {
                SaciError::configuration("udp sink 'broadcast' must be a boolean")
            })?,
        };

        let max_datagram_bytes = match config.get("max_datagram_bytes") {
            None => MAX_DATAGRAM_BYTES,
            Some(value) => value
                .as_u64()
                .and_then(|v| usize::try_from(v).ok())
                .ok_or_else(|| {
                    SaciError::configuration(
                        "udp sink 'max_datagram_bytes' must be a positive integer",
                    )
                })?,
        };

        let probe = parse_probe(config)?;
        let schema = parse_schema_fields(config, "udp")?;
        let transformer = ctx.transformer("udp")?;

        Ok(Box::new(UdpSink::connect(
            &connect,
            broadcast,
            max_datagram_bytes,
            probe,
            schema,
            transformer,
        )?))
    }

    /// Nothing is buffered, so a rebuild loses no datagram the sink had
    /// accepted. A fresh instance re-resolves every `connect` address and
    /// opens fresh sockets, which is the whole of what reconnecting means for
    /// UDP.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Ok(())
    }
}

/// Read `connect` as one address or a list of them.
///
/// KDL gives a node with one argument a scalar and a node with several an
/// array, so `connect "a:1"` and `connect="a:1"` are strings while
/// `connect "a:1" "b:2"` is an array.
fn parse_connect(config: &ConfigValue) -> Result<Vec<String>, SaciError> {
    let refusal = || {
        SaciError::configuration(
            "udp sink config requires 'connect', one address string or a list of them",
        )
    };
    match config.get("connect") {
        Some(ConfigValue::String(one)) => Ok(vec![one.clone()]),
        Some(ConfigValue::Array(many)) if !many.is_empty() => many
            .iter()
            .map(|v| v.as_str().map(str::to_string).ok_or_else(refusal))
            .collect(),
        _ => Err(refusal()),
    }
}

/// Read the optional `probe` node.
///
/// A bare `probe` node parses to an empty object, so the line alone enables
/// the defaults.
fn parse_probe(config: &ConfigValue) -> Result<Option<ProbeSettings>, SaciError> {
    let Some(value) = config.get("probe") else {
        return Ok(None);
    };
    let ConfigValue::Object(_) = value else {
        return Err(SaciError::configuration(
            "udp sink 'probe' must be a node with optional kind, interval_ms, timeout_ms and on_failure properties",
        ));
    };

    let kind = match probe_str(value, "kind", "echo")? {
        "echo" => ProbeKind::Echo,
        "icmp" => ProbeKind::Icmp,
        other => {
            return Err(SaciError::configuration(format!(
                "udp sink probe 'kind' must be \"echo\" or \"icmp\", got '{other}'"
            )));
        }
    };
    let interval = probe_millis(value, "interval_ms", DEFAULT_PROBE_INTERVAL_MS)?;
    let timeout = probe_millis(value, "timeout_ms", DEFAULT_PROBE_TIMEOUT_MS)?;
    let on_failure = match probe_str(value, "on_failure", "refuse")? {
        "refuse" => OnFailure::Refuse,
        "warn" => OnFailure::Warn,
        other => {
            return Err(SaciError::configuration(format!(
                "udp sink probe 'on_failure' must be \"refuse\" or \"warn\", got '{other}'"
            )));
        }
    };

    Ok(Some(ProbeSettings {
        kind,
        interval: Duration::from_millis(interval),
        timeout: Duration::from_millis(timeout),
        on_failure,
    }))
}

/// Read one optional string property of the `probe` node.
///
/// A property that is present with another type is a typo rather than a
/// request for the default, which is why it is refused instead of ignored.
fn probe_str<'a>(
    value: &'a ConfigValue,
    key: &str,
    default: &'a str,
) -> Result<&'a str, SaciError> {
    match value.get(key) {
        None => Ok(default),
        Some(raw) => raw.as_str().ok_or_else(|| {
            SaciError::configuration(format!("udp sink probe '{key}' must be a string"))
        }),
    }
}

/// Read one optional millisecond count of the `probe` node.
///
/// A negative or non-numeric value is refused for the same reason: the zero
/// both keys already refuse is a value, and a typo is not.
fn probe_millis(value: &ConfigValue, key: &str, default: u64) -> Result<u64, SaciError> {
    match value.get(key) {
        None => Ok(default),
        Some(raw) => raw.as_u64().ok_or_else(|| {
            SaciError::configuration(format!("udp sink probe '{key}' must be a positive integer"))
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use saci_connector::{ConfigMap, from_kdl_str};
    use saci_transformer::{Transformer, TransformerFactory};
    use saci_transformer_arrow_ipc::ArrowIpcTransformerFactory;
    use std::sync::Arc;

    /// A built transformer, the way the host hands one to a factory.
    fn transformer() -> Arc<dyn Transformer> {
        ArrowIpcTransformerFactory
            .build(&ConfigValue::Object(ConfigMap::new()))
            .expect("build arrow-ipc transformer")
    }

    fn config(extra: &str) -> ConfigValue {
        let raw = format!(
            r#"
bind "127.0.0.1:0"
{extra}

schema_fields "v" type="Int64" nullable=#false
"#
        );
        from_kdl_str(&raw).expect("parse test config")
    }

    #[test]
    fn builds_with_defaults() {
        let source = UdpSourceFactory
            .build(&config(""), &ConnectorContext::new(Some(transformer())))
            .expect("build");
        assert_eq!(source.schema().fields().len(), 1);
    }

    #[test]
    fn missing_bind_is_a_configuration_error() {
        let cfg = from_kdl_str(
            r#"
schema_fields "v" type="Int64"
"#,
        )
        .unwrap();
        let err = UdpSourceFactory
            .build(&cfg, &ConnectorContext::new(None))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'bind'"), "got: {err}");
    }

    #[test]
    fn a_missing_transformer_is_a_configuration_error() {
        let err = UdpSourceFactory
            .build(&config(""), &ConnectorContext::new(None))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.message().contains("'transformer'"), "got: {err}");
    }

    #[test]
    fn missing_schema_fields_is_a_configuration_error() {
        let cfg = from_kdl_str("bind \"127.0.0.1:0\"\n").unwrap();
        let err = UdpSourceFactory
            .build(&cfg, &ConnectorContext::new(None))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("schema_fields"), "got: {err}");
    }

    #[test]
    fn a_bad_multicast_group_is_a_configuration_error() {
        let err = UdpSourceFactory
            .build(
                &config("multicast_group \"not-an-address\""),
                &ConnectorContext::new(Some(transformer())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'multicast_group'"), "got: {err}");
    }

    fn sink_config(extra: &str) -> ConfigValue {
        let raw = format!(
            r#"
connect "127.0.0.1:9701"
{extra}

schema_fields "v" type="Int64" nullable=#false
"#
        );
        from_kdl_str(&raw).expect("parse test config")
    }

    #[test]
    fn the_sink_builds_from_connect_and_schema_fields() {
        let sink = UdpSinkFactory
            .build(
                &sink_config(""),
                &ConnectorContext::new(Some(transformer())),
            )
            .expect("build");
        assert_eq!(sink.schema().fields().len(), 1);
    }

    /// One config `type` names both halves; the registry keeps them apart.
    #[test]
    fn both_halves_register_as_udp() {
        assert_eq!(UdpSourceFactory.type_name(), "udp");
        assert_eq!(UdpSinkFactory.type_name(), "udp");
    }

    #[test]
    fn sink_missing_connect_is_a_configuration_error() {
        let cfg = from_kdl_str(
            r#"
schema_fields "v" type="Int64"
"#,
        )
        .unwrap();
        let err = UdpSinkFactory
            .build(&cfg, &ConnectorContext::new(None))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'connect'"), "got: {err}");
    }

    #[test]
    fn sink_missing_schema_fields_is_a_configuration_error() {
        let cfg = from_kdl_str("connect \"127.0.0.1:9701\"\n").unwrap();
        let err = UdpSinkFactory
            .build(&cfg, &ConnectorContext::new(None))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("schema_fields"), "got: {err}");
    }

    #[test]
    fn a_sink_missing_its_transformer_is_a_configuration_error() {
        let err = UdpSinkFactory
            .build(&sink_config(""), &ConnectorContext::new(None))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.message().contains("'transformer'"), "got: {err}");
    }

    #[test]
    fn a_connect_list_fans_out() {
        let cfg = from_kdl_str(
            r#"
connect "127.0.0.1:9701" "127.0.0.1:9702"

schema_fields "v" type="Int64" nullable=#false
"#,
        )
        .expect("parse test config");
        assert_eq!(
            parse_connect(&cfg).expect("parse connect"),
            vec!["127.0.0.1:9701".to_string(), "127.0.0.1:9702".to_string()]
        );
        UdpSinkFactory
            .build(&cfg, &ConnectorContext::new(Some(transformer())))
            .expect("build");
    }

    /// A bare `probe` node parses to an empty object, which is what makes the
    /// line alone a complete declaration.
    #[test]
    fn a_bare_probe_node_enables_the_defaults() {
        let cfg = sink_config("probe");
        let settings = parse_probe(&cfg)
            .expect("parse probe")
            .expect("a bare node still declares a probe");
        assert_eq!(
            settings,
            ProbeSettings {
                kind: ProbeKind::Echo,
                interval: Duration::from_millis(5000),
                timeout: Duration::from_millis(1000),
                on_failure: OnFailure::Refuse,
            }
        );
        UdpSinkFactory
            .build(&cfg, &ConnectorContext::new(Some(transformer())))
            .expect("build");
    }

    #[test]
    fn an_unknown_probe_kind_is_a_configuration_error() {
        let err = UdpSinkFactory
            .build(
                &sink_config("probe kind=\"ping\""),
                &ConnectorContext::new(Some(transformer())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'kind'"), "got: {err}");
    }

    #[test]
    fn an_unknown_on_failure_is_a_configuration_error() {
        let err = UdpSinkFactory
            .build(
                &sink_config("probe on_failure=\"ignore\""),
                &ConnectorContext::new(Some(transformer())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'on_failure'"), "got: {err}");
    }

    #[test]
    fn a_probe_with_broadcast_is_a_configuration_error() {
        let err = UdpSinkFactory
            .build(
                &sink_config("broadcast #true\nprobe"),
                &ConnectorContext::new(Some(transformer())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'probe'"), "got: {err}");
    }

    #[test]
    fn a_max_datagram_bytes_that_is_not_an_integer_is_a_configuration_error() {
        let err = UdpSinkFactory
            .build(
                &sink_config("max_datagram_bytes \"lots\""),
                &ConnectorContext::new(Some(transformer())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(
            err.to_string().contains("'max_datagram_bytes'"),
            "got: {err}"
        );
    }

    #[test]
    fn a_negative_max_datagram_bytes_is_a_configuration_error() {
        let err = UdpSinkFactory
            .build(
                &sink_config("max_datagram_bytes -1"),
                &ConnectorContext::new(Some(transformer())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(
            err.to_string().contains("'max_datagram_bytes'"),
            "got: {err}"
        );
    }

    #[test]
    fn a_probe_that_is_not_a_node_is_a_configuration_error() {
        let err = UdpSinkFactory
            .build(
                &sink_config("probe \"yes\""),
                &ConnectorContext::new(Some(transformer())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'probe'"), "got: {err}");
    }

    #[test]
    fn a_connect_list_with_a_non_string_entry_is_a_configuration_error() {
        let cfg = from_kdl_str(
            r#"
connect "127.0.0.1:1" 2

schema_fields "v" type="Int64" nullable=#false
"#,
        )
        .expect("parse test config");
        let err = UdpSinkFactory
            .build(&cfg, &ConnectorContext::new(Some(transformer())))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'connect'"), "got: {err}");
    }

    #[test]
    fn a_bare_connect_is_a_configuration_error() {
        let cfg = from_kdl_str(
            r#"
connect

schema_fields "v" type="Int64" nullable=#false
"#,
        )
        .expect("parse test config");
        let err = UdpSinkFactory
            .build(&cfg, &ConnectorContext::new(Some(transformer())))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'connect'"), "got: {err}");
    }

    #[test]
    fn a_probe_with_a_zero_interval_is_a_configuration_error() {
        let err = UdpSinkFactory
            .build(
                &sink_config("probe interval_ms=0"),
                &ConnectorContext::new(Some(transformer())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("interval_ms"), "got: {err}");
    }

    #[test]
    fn a_group_of_the_other_family_is_a_configuration_error() {
        let err = UdpSourceFactory
            .build(
                &config("multicast_group \"ff02::1\""),
                &ConnectorContext::new(Some(transformer())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("same IP family"), "got: {err}");
    }

    #[test]
    fn buffer_below_one_still_builds() {
        UdpSourceFactory
            .build(
                &config("buffer 0"),
                &ConnectorContext::new(Some(transformer())),
            )
            .expect("a buffer of zero must still build, clamped to one internally");
    }

    #[test]
    fn a_probe_kind_of_icmp_builds() {
        let cfg = sink_config("probe kind=\"icmp\"");
        let settings = parse_probe(&cfg)
            .expect("parse probe")
            .expect("a probe node declares a probe");
        assert_eq!(settings.kind, ProbeKind::Icmp);
        UdpSinkFactory
            .build(&cfg, &ConnectorContext::new(Some(transformer())))
            .expect("build");
    }

    #[test]
    fn an_on_failure_of_warn_builds() {
        let cfg = sink_config("probe on_failure=\"warn\"");
        let settings = parse_probe(&cfg)
            .expect("parse probe")
            .expect("a probe node declares a probe");
        assert_eq!(settings.on_failure, OnFailure::Warn);
        UdpSinkFactory
            .build(&cfg, &ConnectorContext::new(Some(transformer())))
            .expect("build");
    }

    #[test]
    fn a_non_string_multicast_group_is_a_configuration_error() {
        let err = UdpSourceFactory
            .build(
                &config("multicast_group 239"),
                &ConnectorContext::new(Some(transformer())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'multicast_group'"), "got: {err}");
    }

    #[test]
    fn a_non_boolean_broadcast_is_a_configuration_error() {
        let err = UdpSinkFactory
            .build(
                &sink_config("broadcast \"true\""),
                &ConnectorContext::new(Some(transformer())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'broadcast'"), "got: {err}");
    }

    #[test]
    fn a_negative_probe_interval_is_a_configuration_error() {
        let err = UdpSinkFactory
            .build(
                &sink_config("probe interval_ms=-5"),
                &ConnectorContext::new(Some(transformer())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'interval_ms'"), "got: {err}");
    }

    #[test]
    fn a_non_integer_probe_timeout_is_a_configuration_error() {
        let err = UdpSinkFactory
            .build(
                &sink_config("probe timeout_ms=\"1s\""),
                &ConnectorContext::new(Some(transformer())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'timeout_ms'"), "got: {err}");
    }

    #[test]
    fn a_non_string_probe_kind_is_a_configuration_error() {
        let err = UdpSinkFactory
            .build(
                &sink_config("probe kind=5"),
                &ConnectorContext::new(Some(transformer())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'kind'"), "got: {err}");
    }

    #[test]
    fn a_non_string_on_failure_is_a_configuration_error() {
        let err = UdpSinkFactory
            .build(
                &sink_config("probe on_failure=1"),
                &ConnectorContext::new(Some(transformer())),
            )
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'on_failure'"), "got: {err}");
    }
}
