//! The service-to-service connector.
//!
//! One [`PeerConnector`] named `peer` builds the listening half for a `source`
//! node and the dialling half for a `sink` node.
//!
//! [`PeerSource`] is a live source: it never reaches EOF, so it only works
//! under standalone stream mode (`run "stream"`). The service config
//! validator rejects `connector="peer"` on a `source` node in any other mode.
//! That rule reads source nodes only, so a `peer` sink runs in every mode.
//!
//! Neither half takes a `codec` key. Arrow IPC is the wire format,
//! fixed: the peer is another SACI service, and a format choice between two
//! ends that both hold `RecordBatch`es would only be a chance to disagree.
//!
//! Both halves need the [`NodeIdentity`](crate::connector::NodeIdentity) the
//! host binds to every connector it builds. The sink announces it and the
//! source labels its series with what the sink announced, so a receiver fed by
//! several services reads them apart.

use crate::connector::schema::deserialize_config;
use crate::connector::{
    ConfigValue, Connector, ConnectorContext, Rebuild, Role, parse_fields, schema_of,
};
use crate::error::SaciError;
use crate::io::{Sink, Source};

use crate::connector::peer::{PeerSink, PeerSinkConfig, PeerSource, PeerSourceConfig};

/// The `peer` connector: [`PeerSource`] and [`PeerSink`].
///
/// # Source config
///
/// The `config` table is [`PeerSourceConfig`]:
/// - `bind` (string, required): listen address, e.g. `"0.0.0.0:9700"`.
/// - `buffer` (usize, optional, default `64`): queued batch capacity.
/// - `max_frame_bytes` (usize, optional, default `8388608`): per-frame cap.
///
/// Beside it, `field` entries (at least one, required) declare the Arrow
/// schema. A peer whose hello declares other fields is refused.
///
/// # Sink config
///
/// The `config` table is [`PeerSinkConfig`]:
/// - `connect` (string or list of strings, required): peer addresses, tried in
///   order until one accepts a session, e.g. `connect "a:9700" "b:9700"`.
/// - `handshake_timeout_ms` (u64, optional, default `5000`): how long to wait
///   for a peer's accept or reject before trying the next peer.
///
/// Beside it, `field` entries (at least one, required) declare the Arrow
/// schema, announced to the peer and refused by it when the two disagree.
///
/// The addresses are resolved here and dialled on the first batch, so peers
/// that are down fail at serve time, not at validate time.
pub struct PeerConnector;

impl Connector for PeerConnector {
    fn name(&self) -> &'static str {
        "peer"
    }

    fn source(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        let cfg: PeerSourceConfig = deserialize_config(config, "PeerSource")?;
        let schema = schema_of(&parse_fields(config, "peer")?)?;
        let identity = ctx.identity("peer")?.clone();

        Ok(Box::new(PeerSource::new(cfg, schema, identity)?))
    }

    fn sink(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError> {
        let cfg: PeerSinkConfig = deserialize_config(config, "PeerSink")?;
        let schema = schema_of(&parse_fields(config, "peer")?)?;
        let identity = ctx.identity("peer")?.clone();

        Ok(Box::new(PeerSink::new(cfg, schema, identity)?))
    }

    /// A source drops its listener before a rebuild, so its bind address is
    /// free again. Batches received but not yet delivered are lost and peers
    /// must redial, which is what a service restart costs too.
    ///
    /// A sink's fresh instance dials again on its first write, and the batch
    /// whose write failed is the runner's to retry either way. A batch the old
    /// socket accepted was already reported as written, so a rebuild does not
    /// send it again.
    fn rebuild(&self, _role: Role, _config: &ConfigValue) -> Rebuild {
        Rebuild::Yes
    }

    /// A listening socket waits for a peer for as long as it is open.
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
    use crate::connector::{NodeIdentity, from_kdl_str};

    /// A context the way the host hands one to a connector: no codec, an
    /// identity bound.
    fn ctx() -> ConnectorContext {
        ConnectorContext::new(None).with_identity(NodeIdentity {
            service: "svc-a".to_string(),
            workflow: "ticks".to_string(),
            node: "node-1".to_string(),
        })
    }

    fn source_config(extra: &str) -> ConfigValue {
        from_kdl_str(&format!(
            r#"
bind "127.0.0.1:0"
{extra}

field "v" type="int64" nullable=#false
"#
        ))
        .expect("parse test config")
    }

    #[test]
    fn the_source_builds_with_defaults() {
        let source = PeerConnector
            .source(&source_config(""), &ctx())
            .expect("build");
        assert_eq!(source.schema().fields().len(), 1);
    }

    #[test]
    fn source_missing_bind_is_a_configuration_error() {
        let cfg = from_kdl_str("field \"v\" type=\"int64\"\n").unwrap();
        let err = PeerConnector
            .source(&cfg, &ctx())
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("`bind`"), "got: {err}");
    }

    #[test]
    fn source_missing_field_is_a_configuration_error() {
        let cfg = from_kdl_str("bind \"127.0.0.1:0\"\n").unwrap();
        let err = PeerConnector
            .source(&cfg, &ctx())
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'field'"), "got: {err}");
    }

    /// One connector name serves both halves.
    #[test]
    fn one_connector_serves_both_roles_as_peer() {
        assert_eq!(PeerConnector.name(), "peer");
        assert_eq!(PeerConnector.roles(), &[Role::Source, Role::Sink]);
    }

    #[test]
    fn a_peer_source_is_live_and_both_roles_are_rebuildable() {
        assert!(PeerConnector.live(&source_config("")));
        assert_eq!(
            PeerConnector.rebuild(Role::Source, &source_config("")),
            Rebuild::Yes
        );
        assert_eq!(
            PeerConnector.rebuild(Role::Sink, &sink_config("")),
            Rebuild::Yes
        );
    }

    #[test]
    fn both_roles_show_bind_and_connect_in_the_dashboard() {
        assert_eq!(
            PeerConnector.detail_keys(Role::Source),
            &["bind", "connect"]
        );
        assert_eq!(PeerConnector.detail_keys(Role::Sink), &["bind", "connect"]);
    }

    #[test]
    fn a_misspelled_source_key_is_named() {
        let err = PeerConnector
            .source(&source_config("bufer 4"), &ctx())
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("bufer"), "got: {err}");
    }

    #[test]
    fn a_zero_buffer_is_refused_not_clamped() {
        let err = PeerConnector
            .source(&source_config("buffer 0"), &ctx())
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(
            err.to_string().contains("'buffer' must be at least 1"),
            "got: {err}"
        );
    }

    fn sink_config(connect: &str) -> ConfigValue {
        from_kdl_str(&format!(
            r#"
{connect}

field "v" type="int64" nullable=#false
"#
        ))
        .expect("parse test config")
    }

    #[test]
    fn the_sink_takes_one_address_or_a_list_of_them() {
        let one = PeerConnector
            .sink(&sink_config(r#"connect "127.0.0.1:9701""#), &ctx())
            .expect("build from one address");
        assert_eq!(one.schema().fields().len(), 1);

        PeerConnector
            .sink(
                &sink_config(r#"connect "127.0.0.1:9701" "127.0.0.1:9702""#),
                &ctx(),
            )
            .expect("build from a list");
    }

    #[test]
    fn sink_missing_connect_is_a_configuration_error() {
        let cfg = from_kdl_str("field \"v\" type=\"int64\"\n").unwrap();
        let err = PeerConnector
            .sink(&cfg, &ctx())
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("`connect`"), "got: {err}");
    }

    /// A `connect` entry that is not a string is the same refusal as none at
    /// all: the key names addresses or it is wrong.
    #[test]
    fn a_non_string_connect_entry_is_a_configuration_error() {
        let cfg = sink_config("connect 9701 9702");
        let err = PeerConnector
            .sink(&cfg, &ctx())
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("connect"), "got: {err}");
    }

    #[test]
    fn a_sink_address_that_does_not_resolve_is_refused_at_build() {
        let cfg = sink_config(r#"connect "no-such-host.invalid:9701""#);
        let err = PeerConnector
            .sink(&cfg, &ctx())
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(
            err.to_string().contains("no-such-host.invalid:9701"),
            "got: {err}"
        );
    }

    /// Both halves need the identity the host binds, and say so by name.
    #[test]
    fn an_unbound_identity_is_a_configuration_error() {
        let bare = ConnectorContext::new(None);
        for err in [
            PeerConnector
                .source(&source_config(""), &bare)
                .err()
                .expect("source build must fail"),
            PeerConnector
                .sink(&sink_config(r#"connect "127.0.0.1:9701""#), &bare)
                .err()
                .expect("sink build must fail"),
        ] {
            assert_eq!(err.category(), "configuration", "got: {err}");
            assert!(err.message().contains("node identity"), "got: {err}");
        }
    }
}
