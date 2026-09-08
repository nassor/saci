//! The TCP source and sink factories.
//!
//! Both register as `type="tcp"`: the registry keeps its source and sink maps
//! apart, so one string names the listening half in a `source` node and the
//! connecting half in a `sink` node.
//!
//! [`TcpIngestSource`] is a live source: it never reaches EOF, so it only works
//! under standalone stream mode (`run_mode kind="stream"`). The service config
//! validator rejects `type="tcp"` on a `source` node in any other mode. That
//! rule reads source nodes only, so a `tcp` sink runs in every mode.
//!
//! Producers write a `u32` big-endian length plus one payload per frame, and
//! [`TcpSink`] writes the same. What the payload bytes are is the transformer's
//! business: the node's `transformer` key names a declared `transformer` node,
//! and that node's format decides how each frame encodes or decodes. There is
//! no default.

use saci_connector::{
    ConfigValue, ConnectorContext, SinkFactory, SourceFactory, parse_schema_fields,
};
use saci_core::error::SaciError;
use saci_core::io::sink::Sink;
use saci_core::io::source::Source;

use crate::{TcpIngestSource, TcpSink};

/// Default number of decoded batches queued before backpressure.
const DEFAULT_BUFFER: usize = 64;
/// Default per-frame size cap (8 MiB).
const DEFAULT_MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

/// Factory for [`TcpIngestSource`].
///
/// Config fields:
/// - `bind` (string, required): listen address, e.g. `"0.0.0.0:9500"`.
/// - `buffer` (usize, optional, default `64`): queued batch capacity.
/// - `max_frame_bytes` (usize, optional, default `8388608`): per-frame cap.
/// - `schema_fields` (list, required): Arrow schema definition.
///
/// Each frame's payload is decoded by the transformer the host bound to this
/// node from its `transformer` key, reached through the [`ConnectorContext`]
/// rather than read out of `config`.
pub struct TcpSourceFactory;

impl SourceFactory for TcpSourceFactory {
    fn type_name(&self) -> &'static str {
        "tcp"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        let bind = config.get("bind").and_then(|v| v.as_str()).ok_or_else(|| {
            SaciError::configuration("tcp source config requires a 'bind' string")
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

        let schema = parse_schema_fields(config, "tcp")?;
        let transformer = ctx.transformer("tcp")?;

        Ok(Box::new(TcpIngestSource::new(
            bind,
            schema,
            buffer,
            max_frame_bytes,
            transformer,
        )?))
    }

    /// The listener is dropped before a rebuild, so its bind address is free
    /// again. Frames decoded but not yet delivered are lost and producers
    /// must redial, which is what a service restart costs too.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Ok(())
    }
}

/// Factory for [`TcpSink`].
///
/// Config fields:
/// - `connect` (string, required): peer address to dial, e.g.
///   `"collector.internal:9500"`.
/// - `schema_fields` (list, required): Arrow schema definition.
///
/// Each batch is encoded by the transformer the host bound to this node from
/// its `transformer` key, reached through the [`ConnectorContext`] rather than
/// read out of `config`. The address is resolved here and dialled on the first
/// batch, so a peer that is down fails at serve time, not at validate time.
pub struct TcpSinkFactory;

impl SinkFactory for TcpSinkFactory {
    fn type_name(&self) -> &'static str {
        "tcp"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError> {
        let connect = config
            .get("connect")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                SaciError::configuration("tcp sink config requires a 'connect' string")
            })?;

        let schema = parse_schema_fields(config, "tcp")?;
        let transformer = ctx.transformer("tcp")?;

        Ok(Box::new(TcpSink::connect(connect, schema, transformer)?))
    }

    /// One socket serves the sink's whole life and a fresh instance dials
    /// again on its first write. A failed write leaves nothing buffered.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Ok(())
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
        let source = TcpSourceFactory
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
        let err = TcpSourceFactory
            .build(&cfg, &ConnectorContext::new(None))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'bind'"), "got: {err}");
    }

    #[test]
    fn a_missing_transformer_is_a_configuration_error() {
        let err = TcpSourceFactory
            .build(&config(""), &ConnectorContext::new(None))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.message().contains("'transformer'"), "got: {err}");
    }

    #[test]
    fn missing_schema_fields_is_a_configuration_error() {
        let cfg = from_kdl_str("bind \"127.0.0.1:0\"\n").unwrap();
        let err = TcpSourceFactory
            .build(&cfg, &ConnectorContext::new(None))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("schema_fields"), "got: {err}");
    }

    fn sink_config() -> ConfigValue {
        from_kdl_str(
            r#"
connect "127.0.0.1:9500"

schema_fields "v" type="Int64" nullable=#false
"#,
        )
        .expect("parse test config")
    }

    #[test]
    fn the_sink_builds_from_connect_and_schema_fields() {
        let sink = TcpSinkFactory
            .build(&sink_config(), &ConnectorContext::new(Some(transformer())))
            .expect("build");
        assert_eq!(sink.schema().fields().len(), 1);
    }

    /// One config `type` names both halves; the registry keeps them apart.
    #[test]
    fn both_halves_register_as_tcp() {
        assert_eq!(TcpSourceFactory.type_name(), "tcp");
        assert_eq!(TcpSinkFactory.type_name(), "tcp");
    }

    #[test]
    fn sink_missing_connect_is_a_configuration_error() {
        let cfg = from_kdl_str(
            r#"
schema_fields "v" type="Int64"
"#,
        )
        .unwrap();
        let err = TcpSinkFactory
            .build(&cfg, &ConnectorContext::new(None))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("'connect'"), "got: {err}");
    }

    #[test]
    fn sink_missing_schema_fields_is_a_configuration_error() {
        let cfg = from_kdl_str("connect \"127.0.0.1:9500\"\n").unwrap();
        let err = TcpSinkFactory
            .build(&cfg, &ConnectorContext::new(None))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("schema_fields"), "got: {err}");
    }

    #[test]
    fn a_sink_missing_its_transformer_is_a_configuration_error() {
        let err = TcpSinkFactory
            .build(&sink_config(), &ConnectorContext::new(None))
            .err()
            .expect("build must fail");
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.message().contains("'transformer'"), "got: {err}");
    }
}
