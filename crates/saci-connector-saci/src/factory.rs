//! The service-to-service source and sink factories.
//!
//! Both register as `type="saci"`: the registry keeps its source and sink maps
//! apart, so one string names the listening half in a `source` node and the
//! dialling half in a `sink` node.
//!
//! [`SaciSource`] is a live source: it never reaches EOF, so it only works
//! under standalone stream mode (`run_mode kind="stream"`). The service config
//! validator rejects `type="saci"` on a `source` node in any other mode. That
//! rule reads source nodes only, so a `saci` sink runs in every mode.
//!
//! Neither half takes a `transformer` key. Arrow IPC is the wire format,
//! fixed: the peer is another SACI service, and a format choice between two
//! ends that both hold `RecordBatch`es would only be a chance to disagree.
//!
//! Both halves need the [`NodeIdentity`](saci_connector::NodeIdentity) the
//! host binds to every connector it builds. The sink announces it and the
//! source labels its series with what the sink announced, so a receiver fed by
//! several services reads them apart.

use std::time::Duration;

use saci_connector::{
    ConfigValue, ConnectorContext, SinkFactory, SourceFactory, parse_schema_fields,
};
use saci_core::error::SaciError;
use saci_core::io::sink::Sink;
use saci_core::io::source::Source;

use crate::{SaciSink, SaciSource};

/// Default number of received batches queued before backpressure.
const DEFAULT_BUFFER: usize = 64;
/// Default per-frame size cap (8 MiB).
const DEFAULT_MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
/// Default wait for a peer's accept or reject.
const DEFAULT_HANDSHAKE_TIMEOUT_MS: u64 = 5_000;

/// Factory for [`SaciSource`].
///
/// Config fields:
/// - `bind` (string, required): listen address, e.g. `"0.0.0.0:9700"`.
/// - `buffer` (usize, optional, default `64`): queued batch capacity.
/// - `max_frame_bytes` (usize, optional, default `8388608`): per-frame cap.
/// - `schema_fields` (list, required): Arrow schema definition. A peer whose
///   hello declares other fields is refused.
pub struct SaciSourceFactory;

impl SourceFactory for SaciSourceFactory {
    fn type_name(&self) -> &'static str {
        "saci"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        let bind = config.get("bind").and_then(|v| v.as_str()).ok_or_else(|| {
            SaciError::configuration("saci source config requires a 'bind' string")
        })?;

        let buffer = config
            .get("buffer")
            .and_then(|v| v.as_i64())
            .map(|v| v.max(1) as usize)
            .unwrap_or(DEFAULT_BUFFER);

        let max_frame_bytes = config
            .get("max_frame_bytes")
            .and_then(|v| v.as_i64())
            .map(|v| v.max(1) as usize)
            .unwrap_or(DEFAULT_MAX_FRAME_BYTES);

        let schema = parse_schema_fields(config, "saci")?;
        let identity = ctx.identity("saci")?.clone();

        Ok(Box::new(SaciSource::bind(
            bind,
            schema,
            buffer,
            max_frame_bytes,
            identity,
        )?))
    }

    /// The listener is dropped before a rebuild, so its bind address is free
    /// again. Batches received but not yet delivered are lost and peers must
    /// redial, which is what a service restart costs too.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Ok(())
    }
}

/// Factory for [`SaciSink`].
///
/// Config fields:
/// - `connect` (string or list of strings, required): peer addresses, tried in
///   order until one accepts a session, e.g. `connect "a:9700" "b:9700"`.
/// - `handshake_timeout_ms` (u64, optional, default `5000`): how long to wait
///   for a peer's accept or reject before trying the next peer.
/// - `schema_fields` (list, required): Arrow schema definition, announced to
///   the peer and refused by it when the two disagree.
///
/// The addresses are resolved here and dialled on the first batch, so peers
/// that are down fail at serve time, not at validate time.
pub struct SaciSinkFactory;

impl SinkFactory for SaciSinkFactory {
    fn type_name(&self) -> &'static str {
        "saci"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError> {
        let connect = parse_connect(config)?;

        let handshake_timeout = config
            .get("handshake_timeout_ms")
            .and_then(|v| v.as_i64())
            .map(|v| v.max(1) as u64)
            .unwrap_or(DEFAULT_HANDSHAKE_TIMEOUT_MS);

        let schema = parse_schema_fields(config, "saci")?;
        let identity = ctx.identity("saci")?.clone();

        Ok(Box::new(SaciSink::connect(
            &connect,
            schema,
            identity,
            Duration::from_millis(handshake_timeout),
        )?))
    }

    /// A fresh instance dials again on its first write, and the batch whose
    /// write failed is the runner's to retry either way. A batch the old
    /// socket accepted was already reported as written, so a rebuild does not
    /// send it again.
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
            "saci sink config requires 'connect', one address string or a list of them",
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

#[cfg(test)]
mod tests {
    use super::*;
    use saci_connector::{NodeIdentity, from_kdl_str};

    /// A context the way the host hands one to a factory: no transformer, an
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

schema_fields "v" type="Int64" nullable=#false
"#
        ))
        .expect("parse test config")
    }

    #[test]
    fn the_source_builds_with_defaults() {
        let source = SaciSourceFactory
            .build(&source_config(""), &ctx())
            .expect("build");
        assert_eq!(source.schema().fields().len(), 1);
    }

    #[test]
    fn source_missing_bind_is_a_configuration_error() {
        let cfg = from_kdl_str("schema_fields \"v\" type=\"Int64\"\n").unwrap();
        let err = SaciSourceFactory
            .build(&cfg, &ctx())
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'bind'"), "got: {err}");
    }

    #[test]
    fn source_missing_schema_fields_is_a_configuration_error() {
        let cfg = from_kdl_str("bind \"127.0.0.1:0\"\n").unwrap();
        let err = SaciSourceFactory
            .build(&cfg, &ctx())
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("schema_fields"), "got: {err}");
    }

    /// One config `type` names both halves; the registry keeps them apart.
    #[test]
    fn both_halves_register_as_saci() {
        assert_eq!(SaciSourceFactory.type_name(), "saci");
        assert_eq!(SaciSinkFactory.type_name(), "saci");
    }

    fn sink_config(connect: &str) -> ConfigValue {
        from_kdl_str(&format!(
            r#"
{connect}

schema_fields "v" type="Int64" nullable=#false
"#
        ))
        .expect("parse test config")
    }

    #[test]
    fn the_sink_takes_one_address_or_a_list_of_them() {
        let one = SaciSinkFactory
            .build(&sink_config(r#"connect "127.0.0.1:9701""#), &ctx())
            .expect("build from one address");
        assert_eq!(one.schema().fields().len(), 1);

        SaciSinkFactory
            .build(
                &sink_config(r#"connect "127.0.0.1:9701" "127.0.0.1:9702""#),
                &ctx(),
            )
            .expect("build from a list");
    }

    #[test]
    fn sink_missing_connect_is_a_configuration_error() {
        let cfg = from_kdl_str("schema_fields \"v\" type=\"Int64\"\n").unwrap();
        let err = SaciSinkFactory
            .build(&cfg, &ctx())
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'connect'"), "got: {err}");
    }

    /// A `connect` entry that is not a string is the same refusal as none at
    /// all: the key names addresses or it is wrong.
    #[test]
    fn a_non_string_connect_entry_is_a_configuration_error() {
        let cfg = sink_config("connect 9701 9702");
        let err = SaciSinkFactory
            .build(&cfg, &ctx())
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'connect'"), "got: {err}");
    }

    #[test]
    fn a_sink_address_that_does_not_resolve_is_refused_at_build() {
        let cfg = sink_config(r#"connect "no-such-host.invalid:9701""#);
        let err = SaciSinkFactory
            .build(&cfg, &ctx())
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
            SaciSourceFactory
                .build(&source_config(""), &bare)
                .err()
                .expect("source build must fail"),
            SaciSinkFactory
                .build(&sink_config(r#"connect "127.0.0.1:9701""#), &bare)
                .err()
                .expect("sink build must fail"),
        ] {
            assert_eq!(err.category(), "configuration", "got: {err}");
            assert!(err.message().contains("node identity"), "got: {err}");
        }
    }
}
