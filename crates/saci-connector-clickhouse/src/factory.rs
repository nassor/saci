//! Factories for [`ClickhouseSource`] and [`ClickhouseSink`].

use saci_connector::{ConfigValue, ConnectorContext, SinkFactory, SourceFactory};
use saci_core::error::SaciError;
use saci_core::io::sink::Sink;
use saci_core::io::source::Source;
use serde::Deserialize;

use crate::config::{ClickhouseSinkConfig, ClickhouseSourceConfig};
use crate::sink::ClickhouseSink;
use crate::source::ClickhouseSource;

/// Factory for [`ClickhouseSource`].
pub struct ClickhouseSourceFactory;

impl SourceFactory for ClickhouseSourceFactory {
    fn type_name(&self) -> &'static str {
        "ClickhouseSource"
    }

    fn build(
        &self,
        config: &ConfigValue,
        _ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        let cfg = ClickhouseSourceConfig::deserialize(config.clone())
            .map_err(|e| SaciError::configuration(format!("ClickhouseSource config: {e}")))?;
        Ok(Box::new(ClickhouseSource::new(cfg)?))
    }

    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Ok(())
    }
}

/// Factory for [`ClickhouseSink`].
pub struct ClickhouseSinkFactory;

impl SinkFactory for ClickhouseSinkFactory {
    fn type_name(&self) -> &'static str {
        "ClickhouseSink"
    }

    fn build(
        &self,
        config: &ConfigValue,
        _ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError> {
        let cfg = ClickhouseSinkConfig::deserialize(config.clone())
            .map_err(|e| SaciError::configuration(format!("ClickhouseSink config: {e}")))?;
        Ok(Box::new(ClickhouseSink::new(cfg)?))
    }

    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Err(
            "rows accepted but not yet flushed stay buffered in the sink; \
             a second build would discard them",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use saci_connector::from_kdl_str;

    const POLLING_SOURCE: &str = r#"
name "ch_events"
batch_rows 1024

connection url="http://localhost:8123" database="test"

mode kind="polling" table="events" cursor_column="id"

schema_fields "id" type="uint64" nullable=#false
schema_fields "title" type="utf8"
schema_fields "score" type="float64"
"#;

    const DUMP_SOURCE: &str = r#"
name "ch_dump"
batch_rows 500

connection url="http://localhost:8123"

mode kind="dump" table="events"

schema_fields "id" type="uint64" nullable=#false
"#;

    const QUERY_SOURCE: &str = r#"
name "ch_query"

connection url="http://localhost:8123"

mode kind="query" query="SELECT id FROM events WHERE score > 10"

schema_fields "id" type="uint64" nullable=#false
"#;

    const SINK: &str = r#"
name "ch_out"
table "events_out"
truncate_before_first_write #true

connection url="http://localhost:8123" database="test"

schema_fields "id" type="uint64" nullable=#false
schema_fields "title" type="utf8"
"#;

    fn parse_config(text: &str) -> ConfigValue {
        from_kdl_str(text).expect("valid KDL")
    }

    #[test]
    fn factory_type_names_match() {
        assert_eq!(ClickhouseSourceFactory.type_name(), "ClickhouseSource");
        assert_eq!(ClickhouseSinkFactory.type_name(), "ClickhouseSink");
    }

    #[test]
    fn rebuildable_contract_is_accurate() {
        assert!(
            ClickhouseSourceFactory
                .rebuildable(&ConfigValue::Null)
                .is_ok()
        );
        assert!(
            ClickhouseSinkFactory
                .rebuildable(&ConfigValue::Null)
                .is_err()
        );
    }

    #[test]
    fn the_source_factory_builds_polling_source() {
        let source = ClickhouseSourceFactory
            .build(&parse_config(POLLING_SOURCE), &ConnectorContext::new(None))
            .expect("source built");
        let schema = source.schema();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "title");
        assert_eq!(schema.field(2).name(), "score");
    }

    #[test]
    fn the_source_factory_builds_dump_source() {
        let source = ClickhouseSourceFactory
            .build(&parse_config(DUMP_SOURCE), &ConnectorContext::new(None))
            .expect("source built");
        let schema = source.schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "id");
    }

    #[test]
    fn the_source_factory_builds_query_source() {
        let source = ClickhouseSourceFactory
            .build(&parse_config(QUERY_SOURCE), &ConnectorContext::new(None))
            .expect("source built");
        let schema = source.schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "id");
    }

    #[test]
    fn the_sink_factory_builds_sink() {
        let sink = ClickhouseSinkFactory
            .build(&parse_config(SINK), &ConnectorContext::new(None))
            .expect("sink built");
        let schema = sink.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "title");
    }

    #[test]
    fn a_misspelled_key_is_a_configuration_error_naming_it() {
        let bad = POLLING_SOURCE.replace("batch_rows", "batch_size");
        let err = ClickhouseSourceFactory
            .build(&parse_config(&bad), &ConnectorContext::new(None))
            .err()
            .expect("must reject unknown field");
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("batch_size"),
            "error should name misspelled key: {err}"
        );
    }

    #[test]
    fn missing_cursor_column_in_schema_is_rejected() {
        let bad = POLLING_SOURCE.replace("cursor_column=\"id\"", "cursor_column=\"unknown_col\"");
        let err = ClickhouseSourceFactory
            .build(&parse_config(&bad), &ConnectorContext::new(None))
            .err()
            .expect("must reject unknown cursor column");
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("unknown_col"),
            "error should name invalid cursor: {err}"
        );
    }

    #[test]
    fn query_mode_with_explicit_format_is_rejected() {
        let bad = QUERY_SOURCE.replace("WHERE score > 10", "WHERE score > 10 FORMAT Arrow");
        let err = ClickhouseSourceFactory
            .build(&parse_config(&bad), &ConnectorContext::new(None))
            .err()
            .expect("must reject explicit format");
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("FORMAT"),
            "error should mention FORMAT: {err}"
        );
    }

    #[test]
    fn invalid_url_scheme_is_rejected() {
        let bad = SINK.replace("http://localhost:8123", "tcp://localhost:9000");
        let err = ClickhouseSinkFactory
            .build(&parse_config(&bad), &ConnectorContext::new(None))
            .err()
            .expect("must reject non-http url");
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("http://"),
            "error should state expected scheme: {err}"
        );
    }

    #[test]
    fn sink_factory_rejects_misspelled_key() {
        let bad = SINK.replace("table \"events_out\"", "target_table \"events_out\"");
        let err = ClickhouseSinkFactory
            .build(&parse_config(&bad), &ConnectorContext::new(None))
            .err()
            .expect("must reject misspelled key");
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("target_table"),
            "error should name misspelled key: {err}"
        );
    }

    #[test]
    fn sink_factory_rejects_empty_schema_fields() {
        let bad = r#"
name "ch_out"
table "events_out"
connection url="http://localhost:8123"
"#;
        let err = ClickhouseSinkFactory
            .build(&parse_config(bad), &ConnectorContext::new(None))
            .err()
            .expect("must reject empty schema fields");
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("schema_fields"),
            "error should mention schema_fields: {err}"
        );
    }

    #[test]
    fn source_factory_with_tiebreak_column_builds() {
        let with_tie = POLLING_SOURCE.replace(
            "mode kind=\"polling\" table=\"events\" cursor_column=\"id\"",
            "mode kind=\"polling\" table=\"events\" cursor_column=\"id\" tiebreak_column=\"score\"",
        );
        let source = ClickhouseSourceFactory
            .build(&parse_config(&with_tie), &ConnectorContext::new(None))
            .expect("polling source with tiebreak builds");
        assert_eq!(source.schema().fields().len(), 3);
    }

    #[test]
    fn source_factory_rejects_non_cursor_capable_column() {
        let bad = r#"
name "ch_bad_cursor"
connection url="http://localhost:8123"
mode kind="polling" table="events" cursor_column="raw_bytes"
schema_fields "raw_bytes" type="binary"
"#;
        let err = ClickhouseSourceFactory
            .build(&parse_config(bad), &ConnectorContext::new(None))
            .err()
            .expect("must reject binary cursor column");
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("not cursor capable"),
            "error should mention not cursor capable: {err}"
        );
    }

    #[test]
    fn sink_rebuildable_contract_explains_buffering() {
        let err = ClickhouseSinkFactory
            .rebuildable(&ConfigValue::Null)
            .expect_err("sink is not rebuildable");
        assert!(err.contains("rows accepted but not yet flushed"));
    }
}
