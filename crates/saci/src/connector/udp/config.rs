//! The `config` tables of the UDP source and sink.

use std::net::IpAddr;

use serde::Deserialize;

use crate::config::one_or_many;
use crate::connector::schema::at_least_one;
use crate::connector::udp::ProbeConfig;
use crate::connector::udp::sink::MAX_DATAGRAM_BYTES;
use crate::error::SaciError;

/// Default number of decoded batches queued before the receive loop stops
/// reading the socket.
const DEFAULT_BUFFER: usize = 64;

fn default_buffer() -> usize {
    DEFAULT_BUFFER
}

fn default_max_datagram_bytes() -> usize {
    MAX_DATAGRAM_BYTES
}

/// The `config` table of a [`UdpSource`](crate::connector::udp::UdpSource).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UdpSourceConfig {
    /// Receive address, e.g. `"0.0.0.0:9700"`.
    pub bind: String,
    /// A multicast address to join, of the same IP family as `bind`.
    #[serde(default)]
    pub multicast_group: Option<IpAddr>,
    /// Decoded batches that may queue before the receive loop stops reading
    /// the socket. At least 1; default `64`.
    #[serde(default = "default_buffer")]
    pub buffer: usize,
}

impl UdpSourceConfig {
    /// A config receiving on `bind`, with no multicast group and the default
    /// buffer.
    pub fn new(bind: impl Into<String>) -> Self {
        Self {
            bind: bind.into(),
            multicast_group: None,
            buffer: DEFAULT_BUFFER,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), SaciError> {
        at_least_one("udp", "buffer", self.buffer as u64)
    }
}

/// The `config` table of a [`UdpSink`](crate::connector::udp::UdpSink).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UdpSinkConfig {
    /// Peer addresses, e.g. `"collector.internal:9701"`. Every message goes
    /// to every peer. KDL gives one address as a string and several as a
    /// list; both are read.
    #[serde(deserialize_with = "one_or_many")]
    pub connect: Vec<String>,
    /// Enable `SO_BROADCAST` on every socket. Default `false`.
    #[serde(default)]
    pub broadcast: bool,
    /// Per-message cap, `1..=65507`; default `65507`.
    #[serde(default = "default_max_datagram_bytes")]
    pub max_datagram_bytes: usize,
    /// The liveness probe. Absent means no probe; a bare `probe` node means
    /// every default.
    #[serde(default)]
    pub probe: Option<ProbeConfig>,
}

impl UdpSinkConfig {
    /// A config sending to `connect`, with no broadcast, no probe and the
    /// largest datagram.
    pub fn new<I, S>(connect: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            connect: connect.into_iter().map(Into::into).collect(),
            broadcast: false,
            max_datagram_bytes: MAX_DATAGRAM_BYTES,
            probe: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::from_kdl_str;
    use crate::connector::schema::deserialize_config;
    use crate::connector::udp::{OnFailure, ProbeKind};

    #[test]
    fn a_source_table_reads_its_defaults() {
        let table =
            from_kdl_str("bind \"127.0.0.1:0\"\nschema_fields \"v\" type=\"Int64\"\n").unwrap();
        let cfg: UdpSourceConfig = deserialize_config(&table, "UdpSource").unwrap();
        assert_eq!(cfg, UdpSourceConfig::new("127.0.0.1:0"));
    }

    #[test]
    fn a_multicast_group_parses_as_an_ip_address() {
        let table = from_kdl_str("bind \"0.0.0.0:9700\"\nmulticast_group \"239.0.0.1\"\n").unwrap();
        let cfg: UdpSourceConfig = deserialize_config(&table, "UdpSource").unwrap();
        assert_eq!(cfg.multicast_group, Some("239.0.0.1".parse().unwrap()));

        let bad = from_kdl_str("bind \"0.0.0.0:9700\"\nmulticast_group \"nope\"\n").unwrap();
        let err = deserialize_config::<UdpSourceConfig>(&bad, "UdpSource").unwrap_err();
        assert!(err.to_string().contains("multicast_group"), "got: {err}");
    }

    #[test]
    fn a_sink_connect_is_one_address_or_a_list() {
        let one = from_kdl_str("connect \"a:1\"\n").unwrap();
        let cfg: UdpSinkConfig = deserialize_config(&one, "UdpSink").unwrap();
        assert_eq!(cfg, UdpSinkConfig::new(["a:1"]));

        let many = from_kdl_str("connect \"a:1\" \"b:2\"\n").unwrap();
        let cfg: UdpSinkConfig = deserialize_config(&many, "UdpSink").unwrap();
        assert_eq!(cfg.connect, ["a:1", "b:2"]);
    }

    /// A bare `probe` node parses to an empty table, which is what makes the
    /// line alone a complete declaration.
    #[test]
    fn a_bare_probe_node_is_every_default() {
        let table = from_kdl_str("connect \"a:1\"\nprobe\n").unwrap();
        let cfg: UdpSinkConfig = deserialize_config(&table, "UdpSink").unwrap();
        assert_eq!(cfg.probe, Some(ProbeConfig::default()));
        assert_eq!(
            ProbeConfig::default(),
            ProbeConfig {
                kind: ProbeKind::Echo,
                interval_ms: 5000,
                timeout_ms: 1000,
                on_failure: OnFailure::Refuse,
            }
        );
    }

    #[test]
    fn a_probe_keeps_the_properties_it_names() {
        let table = from_kdl_str(
            "connect \"a:1\"\nprobe kind=\"icmp\" interval_ms=250 on_failure=\"warn\"\n",
        )
        .unwrap();
        let cfg: UdpSinkConfig = deserialize_config(&table, "UdpSink").unwrap();
        assert_eq!(
            cfg.probe,
            Some(ProbeConfig {
                kind: ProbeKind::Icmp,
                interval_ms: 250,
                timeout_ms: 1000,
                on_failure: OnFailure::Warn,
            })
        );
    }

    #[test]
    fn a_misspelled_probe_key_is_named() {
        let table = from_kdl_str("connect \"a:1\"\nprobe intervall_ms=250\n").unwrap();
        let err = deserialize_config::<UdpSinkConfig>(&table, "UdpSink").unwrap_err();
        assert!(err.to_string().contains("intervall_ms"), "got: {err}");
    }

    #[test]
    fn a_zero_buffer_is_refused() {
        let cfg = UdpSourceConfig {
            buffer: 0,
            ..UdpSourceConfig::new("127.0.0.1:0")
        };
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(
            err.to_string().contains("udp: 'buffer' must be at least 1"),
            "got: {err}"
        );
    }
}
