//! The OpenTelemetry metrics connector.

use crate::connector::schema::without_fields;
use crate::connector::{
    ConfigValue, Connector, ConnectorContext, Rebuild, Role, parse_optional_fields, schema_of,
};
use crate::error::SaciError;
use crate::io::{Sink, Source};
use serde::Deserialize;

use crate::connector::otlp::config::{OtlpMetricsSinkConfig, OtlpMetricsSourceConfig};
use crate::connector::otlp::sink::OtlpMetricsSink;
use crate::connector::otlp::source::OtlpMetricsSource;

/// The `otlp-metrics` connector: an OTLP/HTTP metrics source and sink.
///
/// The source's `config` table is [`OtlpMetricsSourceConfig`]: the push
/// mode's shorthand keys, or a `mode` block naming `push` or `pull`, and the
/// `field` entries declaring the Arrow schema to emit. The sink's is
/// [`OtlpMetricsSinkConfig`]: the collector's `url`, the body `encoding`, the
/// `headers` every export carries, and the timeouts and counts documented on
/// the config type. No codec is resolved for either role: the connector
/// carries `RecordBatch`es and owns the OTLP wire format itself.
pub struct OtlpMetricsConnector;

impl Connector for OtlpMetricsConnector {
    fn name(&self) -> &'static str {
        super::NAME
    }

    fn source(
        &self,
        config: &ConfigValue,
        _ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        let cfg = OtlpMetricsSourceConfig::deserialize(without_fields(config))
            .map_err(|e| SaciError::configuration(format!("OtlpMetricsSource config: {e}")))?;
        let schema = parse_optional_fields(config, "OtlpMetricsSource")?
            .map(|fields| schema_of(&fields))
            .transpose()?;
        Ok(Box::new(OtlpMetricsSource::new(cfg, schema)?))
    }

    fn sink(
        &self,
        config: &ConfigValue,
        _ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError> {
        let cfg = OtlpMetricsSinkConfig::deserialize(without_fields(config))
            .map_err(|e| SaciError::configuration(format!("OtlpMetricsSink config: {e}")))?;
        let schema = parse_optional_fields(config, "OtlpMetricsSink")?
            .map(|fields| schema_of(&fields))
            .transpose()?;
        Ok(Box::new(OtlpMetricsSink::new(cfg, schema)?))
    }

    /// A second source build binds the same address, which succeeds once the
    /// first instance has released it, and a push source holds nothing a
    /// rebuild would drop: every body it took is already down the channel. A
    /// pull source re-reads its url, which is the at-least-once contract it
    /// already documents. `write_batch` awaits every export it starts, so a
    /// failed write leaves nothing queued for a rebuild to discard.
    fn rebuild(&self, _role: Role, _config: &ConfigValue) -> Rebuild {
        Rebuild::Yes
    }

    /// A push listener has no last request and a pull source repeats unless
    /// `stop_at_end` is set. A source with no `mode` block is the push
    /// listener. Read straight off the table, so a `mode` block the source
    /// would refuse still answers the way the key says.
    fn live(&self, config: &ConfigValue) -> bool {
        let mode = config.get("mode");
        let pull = matches!(
            mode.and_then(|mode| mode.get("kind"))
                .and_then(ConfigValue::as_str),
            Some("pull")
        );
        let stop_at_end = mode
            .and_then(|mode| mode.get("stop_at_end"))
            .and_then(ConfigValue::as_bool)
            .unwrap_or(false);
        !pull || !stop_at_end
    }

    fn detail_keys(&self, role: Role) -> &'static [&'static str] {
        match role {
            Role::Source => &["bind", "path", "mode.kind", "mode.url"],
            Role::Sink => &["url", "encoding", "service_name"],
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::connector::from_kdl_str;

    use super::*;

    fn parse(text: &str) -> ConfigValue {
        from_kdl_str(text).expect("valid KDL")
    }

    /// The error one refused source build reports.
    ///
    /// Written out rather than taken from `expect_err`, which needs the built
    /// value to be `Debug` and a `Box<dyn Source>` is not.
    fn refused_source(config: &ConfigValue) -> SaciError {
        match OtlpMetricsConnector.source(config, &ConnectorContext::new(None)) {
            Ok(_) => panic!("the config is refused"),
            Err(error) => error,
        }
    }

    /// The error one refused sink build reports.
    fn refused_sink(config: &ConfigValue) -> SaciError {
        match OtlpMetricsConnector.sink(config, &ConnectorContext::new(None)) {
            Ok(_) => panic!("the config is refused"),
            Err(error) => error,
        }
    }

    #[test]
    fn the_connector_declares_its_name_roles_and_detail_keys() {
        assert_eq!(OtlpMetricsConnector.name(), "otlp-metrics");
        assert_eq!(OtlpMetricsConnector.roles(), &[Role::Source, Role::Sink]);
        assert_eq!(
            OtlpMetricsConnector.detail_keys(Role::Source),
            &["bind", "path", "mode.kind", "mode.url"]
        );
        assert_eq!(
            OtlpMetricsConnector.detail_keys(Role::Sink),
            &["url", "encoding", "service_name"]
        );
    }

    #[test]
    fn a_push_listener_and_a_repeating_pull_are_live_and_a_bounded_pull_is_not() {
        assert!(OtlpMetricsConnector.live(&parse("bind \"127.0.0.1:0\"")));
        assert!(OtlpMetricsConnector.live(&ConfigValue::Object(Default::default())));
        assert!(OtlpMetricsConnector.live(&parse(
            "mode kind=\"pull\" url=\"http://127.0.0.1:1/\" interval_ms=1000"
        )));
        assert!(!OtlpMetricsConnector.live(&parse(
            "mode kind=\"pull\" url=\"http://127.0.0.1:1/\" stop_at_end=#true interval_ms=0"
        )));
    }

    #[test]
    fn both_halves_declare_a_second_build_sound() {
        assert_eq!(
            OtlpMetricsConnector.rebuild(Role::Source, &ConfigValue::Null),
            Rebuild::Yes
        );
        assert_eq!(
            OtlpMetricsConnector.rebuild(Role::Sink, &ConfigValue::Null),
            Rebuild::Yes
        );
    }

    #[test]
    fn a_source_builds_a_listener_outside_the_host() {
        let config = parse("bind \"127.0.0.1:0\"");
        let source = OtlpMetricsConnector
            .source(&config, &ConnectorContext::new(None))
            .expect("the listener binds");
        // The canonical schema is what a source with no `field` emits.
        assert_eq!(source.schema().fields().len(), 10);
    }

    #[test]
    fn a_source_honours_the_declared_schema() {
        let config = parse(
            "bind \"127.0.0.1:0\"\nfield \"id\" type=\"int64\" nullable=#false\n\
             field \"label\" type=\"utf8\" nullable=#false",
        );
        let source = OtlpMetricsConnector
            .source(&config, &ConnectorContext::new(None))
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
        assert!(error.to_string().contains("OtlpMetricsSource"), "{error}");
    }

    #[test]
    fn a_sink_builds_without_touching_the_endpoint() {
        let config = parse("url \"http://127.0.0.1:1\"");
        let sink = OtlpMetricsConnector
            .sink(&config, &ConnectorContext::new(None))
            .expect("no request is made at build time");
        assert_eq!(sink.schema().fields().len(), 10);
    }

    #[test]
    fn a_sink_reports_the_schema_its_config_declares() {
        let config =
            parse("url \"http://127.0.0.1:1\"\nfield \"id\" type=\"int64\" nullable=#false");
        let sink = OtlpMetricsConnector
            .sink(&config, &ConnectorContext::new(None))
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
