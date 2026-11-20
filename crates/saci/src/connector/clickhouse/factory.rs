//! The ClickHouse [`Connector`].

use crate::connector::{ConfigValue, Connector, ConnectorContext, Rebuild, Role};
use crate::error::SaciError;
use crate::io::{Sink, Source};
use serde::Deserialize;

use crate::connector::clickhouse::config::{ClickhouseSinkConfig, ClickhouseSourceConfig};
use crate::connector::clickhouse::sink::ClickhouseSink;
use crate::connector::clickhouse::source::ClickhouseSource;

/// Connector for ClickHouse, named `clickhouse`: builds [`ClickhouseSource`]
/// and [`ClickhouseSink`].
///
/// A `source` node's `config` table is [`ClickhouseSourceConfig`]: a `name`,
/// a `connection` table, a `mode` table tagged by `kind` (`polling`, `dump` or
/// `query`), and a `field` array declaring the Arrow schema.
///
/// A `sink` node's `config` table is [`ClickhouseSinkConfig`]: a `name`, a
/// `connection` table, the target `table`, and a `field` array.
pub struct ClickhouseConnector;

impl Connector for ClickhouseConnector {
    fn name(&self) -> &'static str {
        "clickhouse"
    }

    fn source(
        &self,
        config: &ConfigValue,
        _ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        let cfg = ClickhouseSourceConfig::deserialize(config.clone())
            .map_err(|e| SaciError::configuration(format!("ClickhouseSource config: {e}")))?;
        Ok(Box::new(ClickhouseSource::new(cfg)?))
    }

    fn sink(
        &self,
        config: &ConfigValue,
        _ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError> {
        let cfg = ClickhouseSinkConfig::deserialize(config.clone())
            .map_err(|e| SaciError::configuration(format!("ClickhouseSink config: {e}")))?;
        Ok(Box::new(ClickhouseSink::new(cfg)?))
    }

    fn rebuild(&self, role: Role, _config: &ConfigValue) -> Rebuild {
        match role {
            Role::Source => Rebuild::Yes,
            Role::Sink => Rebuild::No(
                "rows accepted but not yet flushed stay buffered in the sink; \
                 a second build would discard them",
            ),
        }
    }

    fn detail_keys(&self, role: Role) -> &'static [&'static str] {
        match role {
            Role::Source => &["mode.kind", "mode.table", "mode.query"],
            Role::Sink => &["table", "write_mode"],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::from_kdl_str;

    const POLLING_SOURCE: &str = r#"
name "ch_events"
batch_rows 1024

connection url="http://localhost:8123" database="test"

mode kind="polling" table="events" cursor_column="id"

field "id" type="uint64" nullable=#false
field "title" type="utf8"
field "score" type="float64"
"#;

    const DUMP_SOURCE: &str = r#"
name "ch_dump"
batch_rows 500

connection url="http://localhost:8123"

mode kind="dump" table="events"

field "id" type="uint64" nullable=#false
"#;

    const QUERY_SOURCE: &str = r#"
name "ch_query"

connection url="http://localhost:8123"

mode kind="query" query="SELECT id FROM events WHERE score > 10"

field "id" type="uint64" nullable=#false
"#;

    const SINK: &str = r#"
name "ch_out"
table "events_out"
truncate_before_first_write #true

connection url="http://localhost:8123" database="test"

field "id" type="uint64" nullable=#false
field "title" type="utf8"
"#;

    fn parse_config(text: &str) -> ConfigValue {
        from_kdl_str(text).expect("valid KDL")
    }

    #[test]
    fn the_connector_is_named_clickhouse_and_serves_both_roles() {
        assert_eq!(ClickhouseConnector.name(), "clickhouse");
        assert_eq!(ClickhouseConnector.roles(), &[Role::Source, Role::Sink]);
        assert!(!ClickhouseConnector.live(&parse_config(POLLING_SOURCE)));
        assert_eq!(
            ClickhouseConnector.detail_keys(Role::Source),
            &["mode.kind", "mode.table", "mode.query"]
        );
        assert_eq!(
            ClickhouseConnector.detail_keys(Role::Sink),
            &["table", "write_mode"]
        );
    }

    #[test]
    fn rebuild_contract_is_accurate() {
        assert_eq!(
            ClickhouseConnector.rebuild(Role::Source, &ConfigValue::Null),
            Rebuild::Yes
        );
        assert!(
            ClickhouseConnector
                .rebuild(Role::Sink, &ConfigValue::Null)
                .is_no()
        );
    }

    #[test]
    fn the_source_builds_polling_source() {
        let source = ClickhouseConnector
            .source(&parse_config(POLLING_SOURCE), &ConnectorContext::new(None))
            .expect("source built");
        let schema = source.schema();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "title");
        assert_eq!(schema.field(2).name(), "score");
    }

    #[test]
    fn the_source_builds_dump_source() {
        let source = ClickhouseConnector
            .source(&parse_config(DUMP_SOURCE), &ConnectorContext::new(None))
            .expect("source built");
        let schema = source.schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "id");
    }

    #[test]
    fn the_source_builds_query_source() {
        let source = ClickhouseConnector
            .source(&parse_config(QUERY_SOURCE), &ConnectorContext::new(None))
            .expect("source built");
        let schema = source.schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "id");
    }

    #[test]
    fn the_sink_builds_sink() {
        let sink = ClickhouseConnector
            .sink(&parse_config(SINK), &ConnectorContext::new(None))
            .expect("sink built");
        let schema = sink.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "title");
    }

    #[test]
    fn a_misspelled_key_is_a_configuration_error_naming_it() {
        let bad = POLLING_SOURCE.replace("batch_rows", "batch_size");
        let err = ClickhouseConnector
            .source(&parse_config(&bad), &ConnectorContext::new(None))
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
        let err = ClickhouseConnector
            .source(&parse_config(&bad), &ConnectorContext::new(None))
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
        let err = ClickhouseConnector
            .source(&parse_config(&bad), &ConnectorContext::new(None))
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
        let err = ClickhouseConnector
            .sink(&parse_config(&bad), &ConnectorContext::new(None))
            .err()
            .expect("must reject non-http url");
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("http://"),
            "error should state expected scheme: {err}"
        );
    }

    #[test]
    fn sink_rejects_misspelled_key() {
        let bad = SINK.replace("table \"events_out\"", "target_table \"events_out\"");
        let err = ClickhouseConnector
            .sink(&parse_config(&bad), &ConnectorContext::new(None))
            .err()
            .expect("must reject misspelled key");
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("target_table"),
            "error should name misspelled key: {err}"
        );
    }

    #[test]
    fn sink_rejects_missing_field_entries() {
        let bad = r#"
name "ch_out"
table "events_out"
connection url="http://localhost:8123"
"#;
        let err = ClickhouseConnector
            .sink(&parse_config(bad), &ConnectorContext::new(None))
            .err()
            .expect("must reject empty schema fields");
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("field"),
            "error should mention field: {err}"
        );
    }

    #[test]
    fn source_with_tiebreak_column_builds() {
        let with_tie = POLLING_SOURCE.replace(
            "mode kind=\"polling\" table=\"events\" cursor_column=\"id\"",
            "mode kind=\"polling\" table=\"events\" cursor_column=\"id\" tiebreak_column=\"score\"",
        );
        let source = ClickhouseConnector
            .source(&parse_config(&with_tie), &ConnectorContext::new(None))
            .expect("polling source with tiebreak builds");
        assert_eq!(source.schema().fields().len(), 3);
    }

    #[test]
    fn source_rejects_non_cursor_capable_column() {
        let bad = r#"
name "ch_bad_cursor"
connection url="http://localhost:8123"
mode kind="polling" table="events" cursor_column="raw_bytes"
field "raw_bytes" type="binary"
"#;
        let err = ClickhouseConnector
            .source(&parse_config(bad), &ConnectorContext::new(None))
            .err()
            .expect("must reject binary cursor column");
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("not cursor capable"),
            "error should mention not cursor capable: {err}"
        );
    }

    #[test]
    fn sink_rebuild_contract_explains_buffering() {
        let Rebuild::No(why) = ClickhouseConnector.rebuild(Role::Sink, &ConfigValue::Null) else {
            panic!("sink is not rebuildable");
        };
        assert!(why.contains("rows accepted but not yet flushed"));
    }
}
