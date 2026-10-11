//! Factories for [`OpenTelemetryMetricsSource`] and [`OpenTelemetryMetricsSink`].

use saci_connector::{
    ConfigValue, ConnectorContext, SinkFactory, SourceFactory, parse_optional_schema_fields,
};
use saci_core::error::SaciError;
use saci_core::io::sink::Sink;
use saci_core::io::source::Source;
use serde::Deserialize;

use crate::config::{OpenTelemetryMetricsSinkConfig, OpenTelemetryMetricsSourceConfig};
use crate::sink::OpenTelemetryMetricsSink;
use crate::source::OpenTelemetryMetricsSource;

/// Factory for [`OpenTelemetryMetricsSource`].
///
/// The `config` table is [`OpenTelemetryMetricsSourceConfig`]: the push mode's
/// shorthand keys, or a `mode` block naming `push` or `pull`, and the
/// `schema_fields` entries declaring the Arrow schema to emit. No transformer
/// is resolved: the connector carries `RecordBatch`es and owns the OTLP
/// decoding itself.
pub struct OpenTelemetryMetricsSourceFactory;

impl SourceFactory for OpenTelemetryMetricsSourceFactory {
    fn type_name(&self) -> &'static str {
        "OpenTelemetryMetricsSource"
    }

    fn build(
        &self,
        config: &ConfigValue,
        _ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        let cfg = OpenTelemetryMetricsSourceConfig::deserialize(config.clone()).map_err(|e| {
            SaciError::configuration(format!("OpenTelemetryMetricsSource config: {e}"))
        })?;
        let schema = parse_optional_schema_fields(config, "OpenTelemetryMetricsSource")?;
        Ok(Box::new(OpenTelemetryMetricsSource::new(cfg, schema)?))
    }

    /// A second build binds the same address, which succeeds once the first
    /// instance has released it, and a push source holds nothing a rebuild
    /// would drop: every body it took is already down the channel. A pull
    /// source re-reads its url, which is the at-least-once contract it already
    /// documents.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Ok(())
    }
}

/// Factory for [`OpenTelemetryMetricsSink`].
///
/// The `config` table is [`OpenTelemetryMetricsSinkConfig`]: the collector's
/// `url`, the body `encoding`, the `headers` every export carries, and the
/// timeouts and counts documented on the config type. No transformer is
/// resolved: an OTLP request is the wire format.
pub struct OpenTelemetryMetricsSinkFactory;

impl SinkFactory for OpenTelemetryMetricsSinkFactory {
    fn type_name(&self) -> &'static str {
        "OpenTelemetryMetricsSink"
    }

    fn build(
        &self,
        config: &ConfigValue,
        _ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError> {
        let cfg = OpenTelemetryMetricsSinkConfig::deserialize(config.clone()).map_err(|e| {
            SaciError::configuration(format!("OpenTelemetryMetricsSink config: {e}"))
        })?;
        let schema = parse_optional_schema_fields(config, "OpenTelemetryMetricsSink")?;
        Ok(Box::new(OpenTelemetryMetricsSink::new(cfg, schema)?))
    }

    /// `write_batch` awaits every export it starts, so a failed write leaves
    /// nothing queued for a rebuild to discard.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use saci_connector::from_kdl_str;

    use super::*;

    fn parse(text: &str) -> ConfigValue {
        from_kdl_str(text).expect("valid KDL")
    }

    /// The error one refused source build reports.
    ///
    /// Written out rather than taken from `expect_err`, which needs the built
    /// value to be `Debug` and a `Box<dyn Source>` is not.
    fn refused_source(config: &ConfigValue) -> SaciError {
        match OpenTelemetryMetricsSourceFactory.build(config, &ConnectorContext::new(None)) {
            Ok(_) => panic!("the config is refused"),
            Err(error) => error,
        }
    }

    /// The error one refused sink build reports.
    fn refused_sink(config: &ConfigValue) -> SaciError {
        match OpenTelemetryMetricsSinkFactory.build(config, &ConnectorContext::new(None)) {
            Ok(_) => panic!("the config is refused"),
            Err(error) => error,
        }
    }

    #[test]
    fn the_type_names_are_the_strings_a_config_declares() {
        assert_eq!(
            OpenTelemetryMetricsSourceFactory.type_name(),
            "OpenTelemetryMetricsSource"
        );
        assert_eq!(
            OpenTelemetryMetricsSinkFactory.type_name(),
            "OpenTelemetryMetricsSink"
        );
    }

    #[test]
    fn both_halves_declare_a_second_build_sound() {
        assert!(
            OpenTelemetryMetricsSourceFactory
                .rebuildable(&ConfigValue::Null)
                .is_ok()
        );
        assert!(
            OpenTelemetryMetricsSinkFactory
                .rebuildable(&ConfigValue::Null)
                .is_ok()
        );
    }

    #[test]
    fn a_source_builds_a_listener_outside_the_host() {
        let config = parse("bind \"127.0.0.1:0\"");
        let source = OpenTelemetryMetricsSourceFactory
            .build(&config, &ConnectorContext::new(None))
            .expect("the listener binds");
        // The canonical schema is what a source with no `schema_fields` emits.
        assert_eq!(source.schema().fields().len(), 10);
    }

    #[test]
    fn a_source_honours_the_declared_schema() {
        let config = parse(
            "bind \"127.0.0.1:0\"\nschema_fields \"id\" type=\"int64\" nullable=#false\n\
             schema_fields \"label\" type=\"utf8\" nullable=#false",
        );
        let source = OpenTelemetryMetricsSourceFactory
            .build(&config, &ConnectorContext::new(None))
            .expect("the listener binds");
        let schema = source.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "label");
    }

    #[test]
    fn a_source_refuses_a_bind_it_cannot_take() {
        let config = parse("bind \"127.0.0.1:not-a-port\"");
        let error = refused_source(&config);
        assert!(error.to_string().contains("cannot bind"), "{error}");
    }

    #[test]
    fn a_source_refuses_an_unknown_key() {
        let config = parse("bin \"127.0.0.1:0\"");
        let error = refused_source(&config);
        assert!(
            error.to_string().contains("OpenTelemetryMetricsSource"),
            "{error}"
        );
    }

    #[test]
    fn a_sink_builds_without_touching_the_endpoint() {
        let config = parse("url \"http://127.0.0.1:1\"");
        let sink = OpenTelemetryMetricsSinkFactory
            .build(&config, &ConnectorContext::new(None))
            .expect("no request is made at build time");
        assert_eq!(sink.schema().fields().len(), 10);
    }

    #[test]
    fn a_sink_reports_the_schema_its_config_declares() {
        let config = parse(
            "url \"http://127.0.0.1:1\"\nschema_fields \"id\" type=\"int64\" nullable=#false",
        );
        let sink = OpenTelemetryMetricsSinkFactory
            .build(&config, &ConnectorContext::new(None))
            .expect("the sink builds");
        let schema = sink.schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "id");
    }

    #[test]
    fn a_missing_url_is_a_configuration_error() {
        let config = parse("encoding \"json\"");
        let error = refused_sink(&config);
        assert!(error.to_string().contains("url"), "{error}");
    }
}
