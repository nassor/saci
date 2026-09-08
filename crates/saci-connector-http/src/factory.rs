//! The HTTP source and sink factories.
//!
//! Both resolve their byte format through the transformer the host bound to
//! this connector instance via a declared `transformer` key. The schema is
//! where they differ: a source hands the format whatever `schema_fields` says,
//! including nothing, and lets the format decide, while a sink always needs one
//! because it is the schema the rows are written with.
//!
//! Neither factory makes a request, so `saci-service validate` touches no
//! network: an endpoint that is down surfaces on the first batch.

use std::time::Duration;

use saci_connector::{
    ConfigValue, ConnectorContext, SinkFactory, SourceFactory, parse_optional_schema_fields,
    parse_schema_fields,
};
use saci_core::error::SaciError;
use saci_core::io::{sink::Sink, source::Source};

use crate::{HttpSink, HttpSource, SchemaFrom};

/// Whole-request budget when `timeout_ms` is absent.
const DEFAULT_TIMEOUT_MS: i64 = 30_000;
/// Method the sink sends when `method` is absent.
const DEFAULT_METHOD: &str = "POST";

/// The `url` a factory needs, or a configuration error naming the key.
fn required_url<'a>(config: &'a ConfigValue, what: &str) -> Result<&'a str, SaciError> {
    config.get("url").and_then(|v| v.as_str()).ok_or_else(|| {
        SaciError::configuration(format!("{what} config requires a 'url' string field"))
    })
}

/// The whole-request budget, `timeout_ms` milliseconds, defaulted and floored
/// at one millisecond so a zero cannot mean "no timeout at all".
fn timeout(config: &ConfigValue) -> Duration {
    let ms = config
        .get("timeout_ms")
        .and_then(ConfigValue::as_i64)
        .unwrap_or(DEFAULT_TIMEOUT_MS)
        .max(1);
    Duration::from_millis(ms as u64)
}

/// Every `(name, value)` pair the `headers` table holds.
///
/// KDL writes it as a nested node, so `headers { authorization "Bearer x" }`
/// and `headers authorization="Bearer x"` both arrive as a table of scalars.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when `headers` is not a table, or when a
/// value in it is not a string: a header silently dropped for being the wrong
/// shape is worse than a rejected config.
fn headers(config: &ConfigValue, what: &str) -> Result<Vec<(String, String)>, SaciError> {
    let Some(value) = config.get("headers") else {
        return Ok(Vec::new());
    };
    let table = value.as_object().ok_or_else(|| {
        SaciError::configuration(format!(
            "{what} config.headers must be a table of string values"
        ))
    })?;
    table
        .iter()
        .map(|(name, value)| {
            value
                .as_str()
                .map(|value| (name.clone(), value.to_string()))
                .ok_or_else(|| {
                    SaciError::configuration(format!(
                        "{what} config.headers['{name}'] must be a string"
                    ))
                })
        })
        .collect()
}

/// Where the source takes the schema it hands the format from.
///
/// Absent means [`SchemaFrom::Config`], which is what every schemaless format
/// wants.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when `schema_from` is present and is
/// neither `"config"` nor `"body"`: a misspelling silently read as the default
/// would take the body's own schema as a projection target instead of checking
/// it, in production rather than at load.
fn schema_from(config: &ConfigValue, what: &str) -> Result<SchemaFrom, SaciError> {
    let Some(value) = config.get("schema_from") else {
        return Ok(SchemaFrom::Config);
    };
    match value.as_str() {
        Some("config") => Ok(SchemaFrom::Config),
        Some("body") => Ok(SchemaFrom::Body),
        _ => Err(SaciError::configuration(format!(
            "{what} config.schema_from must be \"config\" or \"body\""
        ))),
    }
}

/// Factory for [`HttpSource`].
///
/// Config fields:
/// - `url` (string, required): the resource to GET.
/// - `headers` (table, optional): request headers, `name "value"` per entry.
/// - `timeout_ms` (integer, optional, default `30000`): whole-request budget.
/// - `schema_fields` (list, optional): the declared Arrow schema. Required by
///   `csv`, and by any `schema_from "body"` source; under the default
///   `schema_from "config"` it is a projection target for `parquet`, `avro` and
///   `arrow-ipc`; `ndjson` infers it when absent.
/// - `schema_from` (`"config"` or `"body"`, optional, default `"config"`):
///   where the schema handed to the format comes from. `"config"` hands
///   `schema_fields` to the format, which a self-describing one projects onto;
///   `"body"` withholds it and checks the body's own schema against it field
///   for field.
///
/// The byte format is whatever transformer the `source` node's `transformer`
/// key names; see [`ConnectorContext::transformer`].
pub struct HttpSourceFactory;

impl SourceFactory for HttpSourceFactory {
    fn type_name(&self) -> &'static str {
        "HttpSource"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        let url = required_url(config, "HttpSource")?;
        let transformer = ctx.transformer("HttpSource")?;
        let declared = parse_optional_schema_fields(config, "HttpSource")?;
        Ok(Box::new(HttpSource::new(
            url,
            declared,
            schema_from(config, "HttpSource")?,
            transformer,
            headers(config, "HttpSource")?,
            timeout(config),
        )?))
    }

    /// The source holds no state across calls: a fresh instance re-issues the
    /// same GET, and every request opens its own connection.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Ok(())
    }
}

/// Factory for [`HttpSink`].
///
/// Config fields:
/// - `url` (string, required): the endpoint each batch is sent to.
/// - `method` (string, optional, default `"POST"`): the HTTP method.
/// - `headers` (table, optional): request headers, `name "value"` per entry.
/// - `timeout_ms` (integer, optional, default `30000`): whole-request budget.
/// - `schema_fields` (list, required): the Arrow schema each body is written
///   with.
///
/// The byte format is whatever transformer the `sink` node's `transformer` key
/// names; see [`ConnectorContext::transformer`].
pub struct HttpSinkFactory;

impl SinkFactory for HttpSinkFactory {
    fn type_name(&self) -> &'static str {
        "HttpSink"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError> {
        let url = required_url(config, "HttpSink")?;
        let transformer = ctx.transformer("HttpSink")?;
        let schema = parse_schema_fields(config, "HttpSink")?;
        let method = config
            .get("method")
            .and_then(|v| v.as_str())
            .unwrap_or(DEFAULT_METHOD);
        Ok(Box::new(HttpSink::new(
            url,
            schema,
            transformer,
            method,
            headers(config, "HttpSink")?,
            timeout(config),
        )?))
    }

    /// One self-contained request per batch, nothing buffered between them,
    /// so a fresh instance is indistinguishable from the one it replaced.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use saci_connector::{ConfigMap, from_kdl_str};
    use saci_transformer::{Transformer, TransformerFactory};
    use saci_transformer_csv::CsvTransformerFactory;

    use super::*;

    fn csv_transformer() -> Arc<dyn Transformer> {
        CsvTransformerFactory
            .build(&ConfigValue::Object(ConfigMap::new()))
            .expect("csv transformer builds")
    }

    fn empty_config() -> ConfigValue {
        ConfigValue::Object(ConfigMap::new())
    }

    fn config(raw: &str) -> ConfigValue {
        from_kdl_str(raw).expect("parse test config")
    }

    const SCHEMA: &str = "schema_fields \"id\" type=\"Int64\" nullable=#false\n";

    #[test]
    fn the_type_names_match_the_config_type_key() {
        assert_eq!(HttpSourceFactory.type_name(), "HttpSource");
        assert_eq!(HttpSinkFactory.type_name(), "HttpSink");
    }

    #[test]
    fn a_missing_url_is_a_configuration_error() {
        let ctx = ConnectorContext::new(Some(csv_transformer()));

        let Err(err) = HttpSourceFactory.build(&empty_config(), &ctx) else {
            panic!("url is required");
        };
        assert_eq!(err.category(), "configuration");
        assert_eq!(
            err.message(),
            "HttpSource config requires a 'url' string field"
        );

        let Err(err) = HttpSinkFactory.build(&empty_config(), &ctx) else {
            panic!("url is required");
        };
        assert_eq!(err.category(), "configuration");
        assert_eq!(
            err.message(),
            "HttpSink config requires a 'url' string field"
        );
    }

    /// A misspelling read as the default would project the body onto the
    /// declared schema instead of checking it, on the first batch, in
    /// production.
    #[test]
    fn an_unknown_schema_from_is_a_configuration_error() {
        let ctx = ConnectorContext::new(Some(csv_transformer()));
        let cfg = config(&format!(
            "url \"https://example.invalid/orders.csv\"\nschema_from \"object\"\n{SCHEMA}"
        ));

        let Err(err) = HttpSourceFactory.build(&cfg, &ctx) else {
            panic!("'object' is the s3 spelling, not this connector's");
        };
        assert_eq!(err.category(), "configuration");
        assert_eq!(
            err.message(),
            "HttpSource config.schema_from must be \"config\" or \"body\""
        );
    }

    /// Only a parsed `"body"` reaches the constructor's demand for a declared
    /// schema, so this is what distinguishes it from the default.
    #[test]
    fn schema_from_body_reaches_the_source() {
        let ctx = ConnectorContext::new(Some(csv_transformer()));
        let cfg = config("url \"https://example.invalid/orders.csv\"\nschema_from \"body\"");

        let Err(err) = HttpSourceFactory.build(&cfg, &ctx) else {
            panic!("a 'body' source has nothing to check the body against");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("schema_fields"), "got: {err}");

        HttpSourceFactory
            .build(&config("url \"https://example.invalid/orders.csv\""), &ctx)
            .expect("the default takes no schema at all");
    }

    #[test]
    fn a_source_with_no_bound_transformer_is_a_configuration_error() {
        let ctx = ConnectorContext::new(None);
        let raw = format!("url \"http://127.0.0.1:1/data.csv\"\n{SCHEMA}");

        let Err(err) = HttpSourceFactory.build(&config(&raw), &ctx) else {
            panic!("a source that moves bytes needs a bound transformer");
        };
        assert_eq!(
            err.message(),
            "HttpSource moves bytes and needs a 'transformer' key naming a declared transformer"
        );
    }

    #[test]
    fn a_sink_with_no_bound_transformer_is_a_configuration_error() {
        let ctx = ConnectorContext::new(None);
        let raw = format!("url \"http://127.0.0.1:1/ingest\"\n{SCHEMA}");

        let Err(err) = HttpSinkFactory.build(&config(&raw), &ctx) else {
            panic!("a sink that moves bytes needs a bound transformer");
        };
        assert_eq!(
            err.message(),
            "HttpSink moves bytes and needs a 'transformer' key naming a declared transformer"
        );
    }

    #[test]
    fn a_sink_without_schema_fields_is_a_configuration_error() {
        let ctx = ConnectorContext::new(Some(csv_transformer()));
        let Err(err) = HttpSinkFactory.build(&config("url \"http://127.0.0.1:1/ingest\"\n"), &ctx)
        else {
            panic!("a sink needs the schema it writes");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("schema_fields"), "got: {err}");
    }

    /// The whole point of building without a request: an endpoint that does not
    /// exist still builds, so `validate` never depends on the network.
    #[test]
    fn both_halves_build_against_an_endpoint_that_is_down() {
        let ctx = ConnectorContext::new(Some(csv_transformer()));
        let raw = format!(
            r#"
url "http://127.0.0.1:1/data.csv"
timeout_ms 500
headers {{
    accept "text/csv"
}}
{SCHEMA}"#
        );

        let source = HttpSourceFactory
            .build(&config(&raw), &ctx)
            .expect("source builds");
        assert_eq!(source.schema().fields().len(), 1);
        assert_eq!(source.schema().field(0).name(), "id");

        let sink = HttpSinkFactory
            .build(&config(&raw), &ctx)
            .expect("sink builds");
        assert_eq!(sink.schema().fields().len(), 1);
    }

    #[test]
    fn a_source_without_schema_fields_reports_an_empty_schema() {
        let ctx = ConnectorContext::new(Some(csv_transformer()));
        let source = HttpSourceFactory
            .build(&config("url \"http://127.0.0.1:1/data.csv\"\n"), &ctx)
            .expect("the format decides whether it needs a declared schema");
        assert_eq!(source.schema().fields().len(), 0);
        assert_eq!(source.estimated_rows(), None);
    }

    #[test]
    fn a_non_string_header_value_is_a_configuration_error_naming_the_key() {
        let ctx = ConnectorContext::new(Some(csv_transformer()));
        let raw = format!(
            r#"
url "http://127.0.0.1:1/ingest"
headers {{
    retries 3
}}
{SCHEMA}"#
        );

        let Err(err) = HttpSinkFactory.build(&config(&raw), &ctx) else {
            panic!("a numeric header value is not a header");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'retries'"), "got: {err}");
    }

    #[test]
    fn a_scalar_headers_key_is_a_configuration_error() {
        let ctx = ConnectorContext::new(Some(csv_transformer()));
        let raw = format!("url \"http://127.0.0.1:1/ingest\"\nheaders \"accept\"\n{SCHEMA}");

        let Err(err) = HttpSinkFactory.build(&config(&raw), &ctx) else {
            panic!("a bare scalar is not a header table");
        };
        assert_eq!(
            err.message(),
            "HttpSink config.headers must be a table of string values"
        );
    }

    #[test]
    fn an_invalid_method_is_rejected_by_the_sink_factory() {
        let ctx = ConnectorContext::new(Some(csv_transformer()));
        let raw = format!("url \"http://127.0.0.1:1/ingest\"\nmethod \"PO ST\"\n{SCHEMA}");

        let Err(err) = HttpSinkFactory.build(&config(&raw), &ctx) else {
            panic!("a space is not legal in a method name");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'PO ST'"), "got: {err}");
    }
}
