//! The PostgreSQL [`Connector`].
//!
//! Both deserialise the whole `config` sub-table with serde rather than reading
//! [`ConfigValue`] keys by hand: the connector's configuration is dozens of fields
//! across nested tables, and `#[serde(deny_unknown_fields)]` on every one of
//! them is what turns a misspelled key into a startup error instead of a
//! silently ignored setting.
//!
//! `PostgresSource::new` and `PostgresSink::new` are synchronous and open no
//! connection, so `saci-service validate` stays database-free; `serve` fails on
//! the first `next_batch`/`write_batch` if the server is unreachable.

use serde::Deserialize;

use crate::connector::{ConfigValue, Connector, ConnectorContext, Rebuild, Role};
use crate::error::SaciError;
use crate::io::{Sink, Source};

use crate::connector::postgresql::{
    PostgresSink, PostgresSinkConfig, PostgresSource, PostgresSourceConfig,
};

/// Connector for PostgreSQL, named `postgresql`: builds [`PostgresSource`] and
/// [`PostgresSink`].
///
/// A `source` node's `config` table is [`PostgresSourceConfig`]: a `name`, a
/// `connection` table, a `mode` table tagged by `kind` (`polling`,
/// `cdc_trigger` or `cdc_logical`), and `field` entries declaring the Arrow
/// schema.
///
/// A `sink` node's `config` table is [`PostgresSinkConfig`]: a `name`, a
/// `connection` table, the target `table`, `field` entries, and the
/// `write_mode` (`append`, `upsert` or `ignore_conflicts`) with its conflict
/// columns.
pub struct PostgresConnector;

impl Connector for PostgresConnector {
    fn name(&self) -> &'static str {
        "postgresql"
    }

    fn source(
        &self,
        config: &ConfigValue,
        _ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        // `ConfigValue` is itself a Deserializer, which is the version-robust
        // way to reach a typed config without re-serialising.
        let cfg = PostgresSourceConfig::deserialize(config.clone())
            .map_err(|e| SaciError::configuration(format!("PostgresSource config: {e}")))?;
        Ok(Box::new(PostgresSource::new(cfg)?))
    }

    fn sink(
        &self,
        config: &ConfigValue,
        _ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError> {
        let cfg = PostgresSinkConfig::deserialize(config.clone())
            .map_err(|e| SaciError::configuration(format!("PostgresSink config: {e}")))?;
        Ok(Box::new(PostgresSink::new(cfg)?))
    }

    /// A source keeps its cursor in the offset table and a logical slot
    /// advances only on ack, both server side, so a fresh reader resumes where
    /// the last committed read left off. A sink keeps rows it has accepted but
    /// not yet flushed buffered across an outage and `connection.reconnect`
    /// re-establishes the session itself, so a second build would discard rows
    /// the first can still write.
    fn rebuild(&self, role: Role, _config: &ConfigValue) -> Rebuild {
        match role {
            Role::Source => Rebuild::Yes,
            Role::Sink => Rebuild::No(
                "rows accepted but not yet flushed stay buffered across an outage and \
                 connection.reconnect re-establishes the session itself; a second build \
                 would discard them",
            ),
        }
    }

    fn detail_keys(&self, role: Role) -> &'static [&'static str] {
        match role {
            Role::Source => &["mode.kind", "mode.table"],
            Role::Sink => &["table", "write_mode"],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::from_kdl_str;

    const SOURCE: &str = r#"
name "pg_orders"
batch_rows 512

connection dsn="postgres://user:pw@db:5432/app" sslmode="disable"

mode kind="polling" table="public.orders" cursor_column="id"

field "id" type="int64" nullable=#false
"#;

    const SINK: &str = r#"
name "pg_out"
table "public.enriched"
write_mode "upsert"
conflict_columns "id"

connection dsn="postgres://user:pw@db:5432/app" sslmode="disable"

field "id" type="int64" nullable=#false
field "total" type="float64"
"#;

    fn value(text: &str) -> ConfigValue {
        from_kdl_str(text).expect("parse")
    }

    // PostgreSQL carries its own wire format, so neither role reads a
    // codec and every build below is handed none.

    #[test]
    fn the_connector_is_named_postgresql_and_serves_both_roles() {
        assert_eq!(PostgresConnector.name(), "postgresql");
        assert_eq!(PostgresConnector.roles(), &[Role::Source, Role::Sink]);
        assert!(!PostgresConnector.live(&value(SOURCE)));
        assert_eq!(
            PostgresConnector.detail_keys(Role::Source),
            &["mode.kind", "mode.table"]
        );
        assert_eq!(
            PostgresConnector.detail_keys(Role::Sink),
            &["table", "write_mode"]
        );
    }

    #[test]
    fn the_source_builds_from_its_config_table() {
        let source = PostgresConnector
            .source(&value(SOURCE), &ConnectorContext::new(None))
            .expect("source built");
        let schema = source.schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "id");
    }

    #[test]
    fn the_sink_builds_from_its_config_table() {
        let sink = PostgresConnector
            .sink(&value(SINK), &ConnectorContext::new(None))
            .expect("sink built");
        let schema = sink.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(1).name(), "total");
    }

    #[test]
    fn a_misspelled_key_is_a_configuration_error_naming_it() {
        // `Box<dyn Source>` is not `Debug`, so the rejection is destructured.
        let Err(err) = PostgresConnector.source(
            &value(&SOURCE.replace("batch_rows", "batch_row")),
            &ConnectorContext::new(None),
        ) else {
            panic!("a misspelled key must be rejected");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("batch_row"), "{}", err.message());
    }

    #[test]
    fn a_failed_validation_reaches_the_caller() {
        let Err(err) = PostgresConnector.sink(
            &value(&SINK.replace("conflict_columns \"id\"\n", "")),
            &ConnectorContext::new(None),
        ) else {
            panic!("upsert without conflict columns must be rejected");
        };
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("conflict_columns"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn the_source_may_be_rebuilt_and_the_sink_may_not() {
        assert_eq!(
            PostgresConnector.rebuild(Role::Source, &value(SOURCE)),
            Rebuild::Yes
        );
        let Rebuild::No(reason) = PostgresConnector.rebuild(Role::Sink, &value(SINK)) else {
            panic!("a sink holds buffered rows");
        };
        assert!(reason.contains("buffered"), "{reason}");
    }
}
