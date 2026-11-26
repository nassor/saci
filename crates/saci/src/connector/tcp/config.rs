//! The `config` tables of the TCP source and sink.

use serde::Deserialize;

use crate::connector::schema::at_least_one;
use crate::error::SaciError;

/// Default number of decoded batches queued before backpressure.
const DEFAULT_BUFFER: usize = 64;
/// Default per-frame size cap (8 MiB).
const DEFAULT_MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

fn default_buffer() -> usize {
    DEFAULT_BUFFER
}

fn default_max_frame_bytes() -> usize {
    DEFAULT_MAX_FRAME_BYTES
}

/// The `config` table of a [`TcpSource`](crate::connector::tcp::TcpSource).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TcpSourceConfig {
    /// Listen address, e.g. `"0.0.0.0:9500"`.
    pub bind: String,
    /// Decoded batches that may queue before backpressure reaches the
    /// producers. At least 1; default `64`.
    #[serde(default = "default_buffer")]
    pub buffer: usize,
    /// Cap on a single frame; a producer that announces more has its
    /// connection closed. At least 1; default 8 MiB.
    #[serde(default = "default_max_frame_bytes")]
    pub max_frame_bytes: usize,
    /// Optional TLS / mutual-TLS server configuration (`rustls`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<crate::tls::ServerTlsConfig>,
}

impl TcpSourceConfig {
    /// A config listening on `bind`, with the default buffer and frame cap.
    pub fn new(bind: impl Into<String>) -> Self {
        Self {
            bind: bind.into(),
            buffer: DEFAULT_BUFFER,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            tls: None,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), SaciError> {
        at_least_one("tcp", "buffer", self.buffer as u64)?;
        at_least_one("tcp", "max_frame_bytes", self.max_frame_bytes as u64)
    }
}

/// The `config` table of a [`TcpSink`](crate::connector::tcp::TcpSink).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TcpSinkConfig {
    /// Peer address to dial, e.g. `"collector.internal:9500"`.
    pub connect: String,
    /// Optional TLS / mutual-TLS client configuration (`rustls`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<crate::tls::ClientTlsConfig>,
}

impl TcpSinkConfig {
    /// A config dialling `connect`.
    pub fn new(connect: impl Into<String>) -> Self {
        Self {
            connect: connect.into(),
            tls: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::from_kdl_str;
    use crate::connector::schema::deserialize_config;

    #[test]
    fn a_source_table_reads_its_defaults() {
        let table = from_kdl_str("bind \"127.0.0.1:0\"\nfield \"v\" type=\"int64\"\n").unwrap();
        let cfg: TcpSourceConfig = deserialize_config(&table, "TcpSource").unwrap();
        assert_eq!(cfg, TcpSourceConfig::new("127.0.0.1:0"));
    }

    #[test]
    fn a_sink_table_needs_connect_and_nothing_else() {
        let table =
            from_kdl_str("connect \"collector:9500\"\nfield \"v\" type=\"int64\"\n").unwrap();
        let cfg: TcpSinkConfig = deserialize_config(&table, "TcpSink").unwrap();
        assert_eq!(cfg, TcpSinkConfig::new("collector:9500"));

        let extra = from_kdl_str("connect \"collector:9500\"\nbuffer 4\n").unwrap();
        let err = deserialize_config::<TcpSinkConfig>(&extra, "TcpSink").unwrap_err();
        assert!(err.to_string().contains("buffer"), "got: {err}");
    }

    #[test]
    fn a_wrong_type_names_the_key() {
        let table = from_kdl_str("bind \"127.0.0.1:0\"\nmax_frame_bytes \"big\"\n").unwrap();
        let err = deserialize_config::<TcpSourceConfig>(&table, "TcpSource").unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(err.to_string().contains("max_frame_bytes"), "got: {err}");
    }

    #[test]
    fn zero_is_refused_where_one_is_the_floor() {
        for (buffer, max_frame_bytes, key) in [(0, 1024, "buffer"), (4, 0, "max_frame_bytes")] {
            let cfg = TcpSourceConfig {
                buffer,
                max_frame_bytes,
                ..TcpSourceConfig::new("127.0.0.1:0")
            };
            let err = cfg.validate().unwrap_err();
            assert_eq!(err.category(), "configuration");
            assert!(
                err.to_string()
                    .contains(&format!("tcp: '{key}' must be at least 1")),
                "got: {err}"
            );
        }
    }
}
