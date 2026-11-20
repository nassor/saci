//! The `config` tables of the peer source and sink.

use serde::Deserialize;

use crate::config::one_or_many;
use crate::connector::schema::at_least_one;
use crate::error::SaciError;

/// Default number of received batches queued before backpressure.
const DEFAULT_BUFFER: usize = 64;
/// Default per-frame size cap (8 MiB).
const DEFAULT_MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
/// Default wait for a peer's accept or reject.
const DEFAULT_HANDSHAKE_TIMEOUT_MS: u64 = 5_000;

fn default_buffer() -> usize {
    DEFAULT_BUFFER
}

fn default_max_frame_bytes() -> usize {
    DEFAULT_MAX_FRAME_BYTES
}

fn default_handshake_timeout_ms() -> u64 {
    DEFAULT_HANDSHAKE_TIMEOUT_MS
}

/// The `config` table of a [`PeerSource`](crate::connector::peer::PeerSource).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerSourceConfig {
    /// Listen address, e.g. `"0.0.0.0:9700"`.
    pub bind: String,
    /// Batches that may queue before backpressure reaches the sending
    /// services. At least 1; default `64`.
    #[serde(default = "default_buffer")]
    pub buffer: usize,
    /// Cap on a single frame; a peer that announces more has its session
    /// closed. At least 1; default 8 MiB.
    #[serde(default = "default_max_frame_bytes")]
    pub max_frame_bytes: usize,
}

impl PeerSourceConfig {
    /// A config listening on `bind`, with the default buffer and frame cap.
    pub fn new(bind: impl Into<String>) -> Self {
        Self {
            bind: bind.into(),
            buffer: DEFAULT_BUFFER,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), SaciError> {
        at_least_one("peer", "buffer", self.buffer as u64)?;
        at_least_one("peer", "max_frame_bytes", self.max_frame_bytes as u64)
    }
}

/// The `config` table of a [`PeerSink`](crate::connector::peer::PeerSink).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerSinkConfig {
    /// Peer addresses, tried in order until one accepts a session, e.g.
    /// `connect "a:9700" "b:9700"`. KDL gives one address as a string and
    /// several as a list; both are read.
    #[serde(deserialize_with = "one_or_many")]
    pub connect: Vec<String>,
    /// How long to wait for a peer's accept or reject before trying the next
    /// peer. At least 1; default `5000`.
    #[serde(default = "default_handshake_timeout_ms")]
    pub handshake_timeout_ms: u64,
}

impl PeerSinkConfig {
    /// A config dialling `connect` in order, with the default handshake
    /// timeout.
    pub fn new<I, S>(connect: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            connect: connect.into_iter().map(Into::into).collect(),
            handshake_timeout_ms: DEFAULT_HANDSHAKE_TIMEOUT_MS,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), SaciError> {
        at_least_one("peer", "handshake_timeout_ms", self.handshake_timeout_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::from_kdl_str;
    use crate::connector::schema::deserialize_config;

    #[test]
    fn a_source_table_reads_its_defaults() {
        let table =
            from_kdl_str("bind \"127.0.0.1:0\"\nschema_fields \"v\" type=\"Int64\"\n").unwrap();
        let cfg: PeerSourceConfig = deserialize_config(&table, "PeerSource").unwrap();
        assert_eq!(cfg, PeerSourceConfig::new("127.0.0.1:0"));
    }

    #[test]
    fn a_sink_connect_is_one_address_or_a_list() {
        let one = from_kdl_str("connect \"a:1\"\n").unwrap();
        let cfg: PeerSinkConfig = deserialize_config(&one, "PeerSink").unwrap();
        assert_eq!(cfg.connect, ["a:1"]);

        let many = from_kdl_str("connect \"a:1\" \"b:2\"\nhandshake_timeout_ms 250\n").unwrap();
        let cfg: PeerSinkConfig = deserialize_config(&many, "PeerSink").unwrap();
        assert_eq!(cfg.connect, ["a:1", "b:2"]);
        assert_eq!(cfg.handshake_timeout_ms, 250);
    }

    #[test]
    fn a_misspelled_key_is_named() {
        let table = from_kdl_str("bind \"127.0.0.1:0\"\nbufer 4\n").unwrap();
        let err = deserialize_config::<PeerSourceConfig>(&table, "PeerSource").unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(err.to_string().contains("bufer"), "got: {err}");
    }

    #[test]
    fn a_wrong_type_names_the_key() {
        let table = from_kdl_str("bind \"127.0.0.1:0\"\nbuffer \"lots\"\n").unwrap();
        let err = deserialize_config::<PeerSourceConfig>(&table, "PeerSource").unwrap_err();
        assert!(err.to_string().contains("buffer"), "got: {err}");
    }

    #[test]
    fn zero_is_refused_where_one_is_the_floor() {
        let mut cfg = PeerSourceConfig::new("127.0.0.1:0");
        cfg.buffer = 0;
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(
            err.to_string().contains("'buffer' must be at least 1"),
            "got: {err}"
        );

        let mut cfg = PeerSinkConfig::new(["a:1"]);
        cfg.handshake_timeout_ms = 0;
        assert!(
            cfg.validate()
                .unwrap_err()
                .to_string()
                .contains("'handshake_timeout_ms' must be at least 1")
        );
    }
}
