//! The S3 source and sink factories.
//!
//! Both resolve their byte format through the transformer the host bound to
//! this connector instance via a declared `transformer` key. Both need the
//! declared `schema_fields`: a source's schema is read at load time by
//! `validate_workflow_graph`, before any request is made, so it cannot be
//! discovered from the bucket, and a sink writes with it.

use serde::Deserialize;

use saci_connector::{
    ConfigValue, ConnectorContext, SinkFactory, SourceFactory, parse_schema_fields,
};
use saci_core::error::SaciError;
use saci_core::io::{sink::Sink, source::Source};

use crate::config::{S3SinkConfig, S3SourceConfig};
use crate::{S3Sink, S3Source};

/// Factory for [`S3Source`].
///
/// Config fields:
/// - `connection` (node, required): endpoint, credentials and client options;
///   see [`S3ConnectionConfig`](crate::S3ConnectionConfig).
/// - `prefix` (string, optional): the path prefix listed. Empty lists the
///   whole bucket.
/// - `schema_from` (`"config"` or `"object"`, optional): where the Arrow
///   schema handed to the format comes from.
/// - `schema_fields` (list, required): the declared Arrow schema.
///
/// The byte format is whatever transformer the `source` node's `transformer`
/// key names; see [`ConnectorContext::transformer`].
pub struct S3SourceFactory;

impl SourceFactory for S3SourceFactory {
    fn type_name(&self) -> &'static str {
        "S3Source"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        let cfg = S3SourceConfig::deserialize(config.clone())
            .map_err(|e| SaciError::configuration(format!("S3Source config: {e}")))?;
        let transformer = ctx.transformer("S3Source")?;
        let schema = parse_schema_fields(config, "S3Source")?;
        Ok(Box::new(S3Source::new(cfg, schema, transformer)?))
    }

    /// The prefix is listed once per instance, so a fresh one re-delivers
    /// every object already read, and every request opens its own connection
    /// anyway, so there is no handle a rebuild would replace.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Err(
            "the prefix is listed once, so a second build re-delivers every object \
             already read; every request already opens its own connection, so there \
             is no handle to replace",
        )
    }
}

/// Factory for [`S3Sink`].
///
/// Config fields:
/// - `connection` (node, required): endpoint, credentials and client options;
///   see [`S3ConnectionConfig`](crate::S3ConnectionConfig).
/// - `prefix` (string, optional): the path prefix every written object lands
///   under.
/// - `suffix` (string, optional): appended to the generated object key.
/// - `flush` (node, optional): the row/byte/age thresholds that close the open
///   object; see [`Flush`](crate::Flush).
/// - `schema_fields` (list, required): the Arrow schema the rows are written
///   with.
///
/// The byte format is whatever transformer the `sink` node's `transformer` key
/// names; see [`ConnectorContext::transformer`].
pub struct S3SinkFactory;

impl SinkFactory for S3SinkFactory {
    fn type_name(&self) -> &'static str {
        "S3Sink"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError> {
        let cfg = S3SinkConfig::deserialize(config.clone())
            .map_err(|e| SaciError::configuration(format!("S3Sink config: {e}")))?;
        let transformer = ctx.transformer("S3Sink")?;
        let schema = parse_schema_fields(config, "S3Sink")?;
        Ok(Box::new(S3Sink::new(cfg, schema, transformer)?))
    }

    /// Rows accepted but not yet uploaded live in the open object and survive
    /// a failed upload, and every upload opens its own connection anyway.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Err(
            "rows accepted but not yet uploaded live in the open object and survive a \
             failed upload; a second build would discard them, and every upload already \
             opens its own connection",
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use saci_connector::{ConfigMap, from_kdl_str};
    use saci_transformer::{Transformer, TransformerFactory};
    use saci_transformer_csv::CsvTransformerFactory;

    use super::*;

    fn empty_config() -> ConfigValue {
        ConfigValue::Object(ConfigMap::new())
    }

    fn config(raw: &str) -> ConfigValue {
        from_kdl_str(raw).expect("parse test config")
    }

    fn csv_transformer() -> Arc<dyn Transformer> {
        CsvTransformerFactory
            .build(&empty_config())
            .expect("csv transformer builds")
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
schema_fields "id" type="Int64" nullable=#false
"#;

    #[test]
    fn the_type_names_match_the_config_type_key() {
        assert_eq!(S3SourceFactory.type_name(), "S3Source");
        assert_eq!(S3SinkFactory.type_name(), "S3Sink");
    }

    #[test]
    fn a_missing_connection_bucket_is_a_configuration_error() {
        let config = config(r#"connection { endpoint "http://127.0.0.1:1" }"#);
        let Err(err) =
            S3SourceFactory.build(&config, &ConnectorContext::new(Some(csv_transformer())))
        else {
            panic!("missing bucket must fail");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("bucket"));
    }

    #[test]
    fn a_missing_transformer_names_the_connector() {
        let config = config(&format!("{CONNECTION}{SCHEMA}"));
        let Err(err) = S3SourceFactory.build(&config, &ConnectorContext::new(None)) else {
            panic!("no transformer bound must fail");
        };
        assert_eq!(err.category(), "configuration");
        assert_eq!(
            err.message(),
            "S3Source moves bytes and needs a 'transformer' key naming a declared transformer"
        );
    }

    #[test]
    fn a_missing_schema_fields_is_a_configuration_error() {
        let config = config(CONNECTION);
        let Err(err) =
            S3SinkFactory.build(&config, &ConnectorContext::new(Some(csv_transformer())))
        else {
            panic!("missing schema must fail");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("schema_fields"));
    }

    #[test]
    fn a_full_config_builds_without_reaching_the_endpoint() {
        let config = config(&format!("{CONNECTION}{SCHEMA}"));
        S3SourceFactory
            .build(&config, &ConnectorContext::new(Some(csv_transformer())))
            .expect("source builds without a request");
        S3SinkFactory
            .build(&config, &ConnectorContext::new(Some(csv_transformer())))
            .expect("sink builds without a request");
    }
}
