//! The S3 connector.
//!
//! Both roles resolve their byte format through the codec the host bound to
//! this connector instance via a declared `codec` key. Both need the
//! declared `field` entries: a source's schema is read at load time by
//! `validate_workflow_graph`, before any request is made, so it cannot be
//! discovered from the bucket, and a sink writes with it.

use serde::Deserialize;

use crate::connector::schema::without_fields;
use crate::connector::{
    ConfigValue, Connector, ConnectorContext, Rebuild, Role, parse_fields, schema_of,
};
use crate::error::SaciError;
use crate::io::{Sink, Source};

use crate::connector::s3::config::{S3SinkConfig, S3SourceConfig};
use crate::connector::s3::{S3Sink, S3Source};

/// The `s3` connector: an object-store source and sink over any S3-compatible
/// endpoint.
///
/// Source config fields:
/// - `connection` (node, required): endpoint, credentials and client options;
///   see [`ConnectionConfig`](crate::connector::s3::ConnectionConfig).
/// - `prefix` (string, optional): the path prefix listed. Empty lists the
///   whole bucket.
/// - `schema_from` (`"config"` or `"payload"`, optional): where the Arrow
///   schema handed to the format comes from.
/// - `field` (one or more, required): the declared Arrow schema.
///
/// Sink config fields:
/// - `connection` (node, required): endpoint, credentials and client options;
///   see [`ConnectionConfig`](crate::connector::s3::ConnectionConfig).
/// - `prefix` (string, optional): the path prefix every written object lands
///   under.
/// - `suffix` (string, optional): appended to the generated object key.
/// - `flush` (node, optional): the row/byte/age thresholds that close the open
///   object; see [`Flush`](crate::connector::s3::Flush).
/// - `field` (one or more, required): the Arrow schema the rows are written
///   with.
///
/// The byte format is whatever codec the node's `codec` key names; see
/// [`ConnectorContext::codec`].
pub struct S3Connector;

impl Connector for S3Connector {
    fn name(&self) -> &'static str {
        super::NAME
    }

    fn source(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        let cfg = S3SourceConfig::deserialize(without_fields(config))
            .map_err(|e| SaciError::configuration(format!("S3Source config: {e}")))?;
        let codec = ctx.codec("S3Source")?;
        let schema = schema_of(&parse_fields(config, "S3Source")?)?;
        let mut src = S3Source::new(cfg, schema, codec)?;
        src.set_skip_corrupt(ctx.skip_corrupt());
        Ok(Box::new(src))
    }

    fn sink(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError> {
        let cfg = S3SinkConfig::deserialize(without_fields(config))
            .map_err(|e| SaciError::configuration(format!("S3Sink config: {e}")))?;
        let codec = ctx.codec("S3Sink")?;
        let schema = schema_of(&parse_fields(config, "S3Sink")?)?;
        Ok(Box::new(S3Sink::new(cfg, schema, codec)?))
    }

    /// Neither role is rebuilt. The prefix is listed once per source
    /// instance, so a fresh one re-delivers every object already read; rows a
    /// sink accepted but has not yet uploaded live in the open object and
    /// survive a failed upload. Every request opens its own connection, so
    /// there is no handle a rebuild would replace.
    fn rebuild(&self, role: Role, _config: &ConfigValue) -> Rebuild {
        match role {
            Role::Source => Rebuild::No(
                "the prefix is listed once, so a second build re-delivers every object \
                 already read; every request already opens its own connection, so there \
                 is no handle to replace",
            ),
            Role::Sink => Rebuild::No(
                "rows accepted but not yet uploaded live in the open object and survive a \
                 failed upload; a second build would discard them, and every upload already \
                 opens its own connection",
            ),
        }
    }

    fn detail_keys(&self, _role: Role) -> &'static [&'static str] {
        &["connection.bucket", "prefix"]
    }
}

#[cfg(all(test, feature = "codec-csv"))]
mod tests {
    use std::sync::Arc;

    use crate::codec::csv::CsvCodecFactory;
    use crate::codec::{Codec, CodecFactory};
    use crate::connector::{ConfigMap, from_kdl_str};

    use super::*;

    fn empty_config() -> ConfigValue {
        ConfigValue::Object(ConfigMap::new())
    }

    fn config(raw: &str) -> ConfigValue {
        from_kdl_str(raw).expect("parse test config")
    }

    fn csv_codec() -> Arc<dyn Codec> {
        CsvCodecFactory
            .build(&empty_config())
            .expect("csv codec builds")
    }

    const CONNECTION: &str = r#"
connection {
    bucket "test"
    endpoint "http://127.0.0.1:1"
    access_key_id "key"
    secret_access_key "secret"
    allow_http #true
}
"#;

    const SCHEMA: &str = r#"
field "id" type="int64" nullable=#false
"#;

    #[test]
    fn the_connector_declares_its_name_roles_and_detail_keys() {
        assert_eq!(S3Connector.name(), "s3");
        assert_eq!(S3Connector.roles(), &[Role::Source, Role::Sink]);
        let keys: &[&str] = &["connection.bucket", "prefix"];
        assert_eq!(S3Connector.detail_keys(Role::Source), keys);
        assert_eq!(S3Connector.detail_keys(Role::Sink), keys);
        assert!(!S3Connector.live(&empty_config()));
        for role in [Role::Source, Role::Sink] {
            assert!(matches!(
                S3Connector.rebuild(role, &empty_config()),
                Rebuild::No(why) if why.contains("every request already opens")
                    || why.contains("every upload already opens")
            ));
        }
    }

    #[test]
    fn a_missing_connection_bucket_is_a_configuration_error() {
        let config = config(r#"connection { endpoint "http://127.0.0.1:1" }"#);
        let Err(err) = S3Connector.source(&config, &ConnectorContext::new(Some(csv_codec())))
        else {
            panic!("missing bucket must fail");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("bucket"));
    }

    #[test]
    fn a_missing_codec_names_the_connector() {
        let config = config(&format!("{CONNECTION}{SCHEMA}"));
        let Err(err) = S3Connector.source(&config, &ConnectorContext::new(None)) else {
            panic!("no codec bound must fail");
        };
        assert_eq!(err.category(), "configuration");
        assert_eq!(
            err.message(),
            "S3Source moves bytes and needs a 'codec' key naming a declared codec"
        );
    }

    #[test]
    fn a_missing_field_is_a_configuration_error() {
        let config = config(CONNECTION);
        let Err(err) = S3Connector.sink(&config, &ConnectorContext::new(Some(csv_codec()))) else {
            panic!("missing schema must fail");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'field'"), "got: {err}");
    }

    #[test]
    fn a_full_config_builds_without_reaching_the_endpoint() {
        let config = config(&format!("{CONNECTION}{SCHEMA}"));
        S3Connector
            .source(&config, &ConnectorContext::new(Some(csv_codec())))
            .expect("source builds without a request");
        S3Connector
            .sink(&config, &ConnectorContext::new(Some(csv_codec())))
            .expect("sink builds without a request");
    }
}
