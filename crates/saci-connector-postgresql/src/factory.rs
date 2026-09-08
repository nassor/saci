//! The PostgreSQL source and sink factories.
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

use saci_connector::{ConfigValue, ConnectorContext, SinkFactory, SourceFactory};
use saci_core::error::SaciError;
use saci_core::io::sink::Sink;
use saci_core::io::source::Source;

use crate::{PostgresSink, PostgresSinkConfig, PostgresSource, PostgresSourceConfig};

/// Factory for [`PostgresSource`].
///
/// The `config` table is [`PostgresSourceConfig`]: a `name`, a `connection`
/// table, a `mode` table tagged by `kind` (`polling`, `cdc_trigger` or
/// `cdc_logical`), and a `schema_fields` array declaring the Arrow schema.
pub struct PostgresSourceFactory;

impl SourceFactory for PostgresSourceFactory {
    fn type_name(&self) -> &'static str {
        "PostgresSource"
    }

    fn build(
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

    /// The cursor lives in the offset table and a logical slot advances only
    /// on ack, both server side, so a fresh reader resumes where the last
    /// committed read left off.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Ok(())
    }
}

/// Factory for [`PostgresSink`].
///
/// The `config` table is [`PostgresSinkConfig`]: a `name`, a `connection`
/// table, the target `table`, a `schema_fields` array, and the `write_mode`
/// (`append`, `upsert` or `ignore_conflicts`) with its conflict columns.
pub struct PostgresSinkFactory;

impl SinkFactory for PostgresSinkFactory {
    fn type_name(&self) -> &'static str {
        "PostgresSink"
    }

    fn build(
        &self,
        config: &ConfigValue,
        _ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError> {
        let cfg = PostgresSinkConfig::deserialize(config.clone())
            .map_err(|e| SaciError::configuration(format!("PostgresSink config: {e}")))?;
        Ok(Box::new(PostgresSink::new(cfg)?))
    }

    /// Rows accepted but not yet flushed stay buffered across an outage and
    /// `connection.reconnect` re-establishes the session itself, so a second
    /// build would discard rows this one can still write.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Err(
            "rows accepted but not yet flushed stay buffered across an outage and \
             connection.reconnect re-establishes the session itself; a second build \
             would discard them",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use saci_connector::from_kdl_str;

    const SOURCE: &str = r#"
name "pg_orders"
batch_rows 512

connection dsn="postgres://user:pw@db:5432/app" sslmode="disable"

mode kind="polling" table="public.orders" cursor_column="id"

schema_fields "id" type="int64" nullable=#false
"#;

    const SINK: &str = r#"
name "pg_out"
table "public.enriched"
write_mode "upsert"
conflict_columns "id"

connection dsn="postgres://user:pw@db:5432/app" sslmode="disable"

schema_fields "id" type="int64" nullable=#false
schema_fields "total" type="float64"
"#;

    fn value(text: &str) -> ConfigValue {
        from_kdl_str(text).expect("parse")
    }

    // PostgreSQL carries its own wire format, so neither factory reads a
    // transformer and every build below is handed none.

    #[test]
    fn the_source_factory_builds_from_its_config_table() {
        let source = PostgresSourceFactory
            .build(&value(SOURCE), &ConnectorContext::new(None))
            .expect("source built");
        assert_eq!(PostgresSourceFactory.type_name(), "PostgresSource");
        let schema = source.schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "id");
    }

    #[test]
    fn the_sink_factory_builds_from_its_config_table() {
        let sink = PostgresSinkFactory
            .build(&value(SINK), &ConnectorContext::new(None))
            .expect("sink built");
        assert_eq!(PostgresSinkFactory.type_name(), "PostgresSink");
        let schema = sink.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(1).name(), "total");
    }

    #[test]
    fn a_misspelled_key_is_a_configuration_error_naming_it() {
        // `Box<dyn Source>` is not `Debug`, so the rejection is destructured.
        let Err(err) = PostgresSourceFactory.build(
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
        let Err(err) = PostgresSinkFactory.build(
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
}
