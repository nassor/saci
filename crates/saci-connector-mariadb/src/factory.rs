//! The MariaDB and MySQL source and sink factories.
//!
//! Both deserialise the whole `config` sub-table with serde rather than reading
//! [`ConfigValue`] keys by hand: the connector's configuration is dozens of fields
//! across nested tables, and `#[serde(deny_unknown_fields)]` on every one of
//! them is what turns a misspelled key into a startup error instead of a
//! silently ignored setting.
//!
//! `MariadbSource::new` and `MariadbSink::new` are synchronous and open no
//! connection, so `saci-service validate` stays database-free; `serve` fails on
//! the first `next_batch` or `write_batch` if the server is unreachable.

use serde::Deserialize;

use saci_connector::{ConfigValue, ConnectorContext, SinkFactory, SourceFactory};
use saci_core::error::SaciError;
use saci_core::io::sink::Sink;
use saci_core::io::source::Source;

use crate::{MariadbSink, MariadbSinkConfig, MariadbSource, MariadbSourceConfig};

/// Factory for [`MariadbSource`].
///
/// The `config` table is [`MariadbSourceConfig`]: a `name`, a `connection`
/// table, a `mode` table tagged by `kind` (`polling`, `cdc_trigger` or
/// `cdc_binlog`), and a `schema_fields` array declaring the Arrow schema.
pub struct MariadbSourceFactory;

impl SourceFactory for MariadbSourceFactory {
    fn type_name(&self) -> &'static str {
        "MariadbSource"
    }

    fn build(
        &self,
        config: &ConfigValue,
        _ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        // `ConfigValue` is itself a Deserializer, which is the version-robust
        // way to reach a typed config without re-serialising.
        let cfg = MariadbSourceConfig::deserialize(config.clone())
            .map_err(|e| SaciError::configuration(format!("MariadbSource config: {e}")))?;
        Ok(Box::new(MariadbSource::new(cfg)?))
    }

    /// The offset lives in the offset table, server side, so a fresh reader
    /// resumes where the last committed read left off.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Ok(())
    }
}

/// Factory for [`MariadbSink`].
///
/// The `config` table is [`MariadbSinkConfig`]: a `name`, a `connection`
/// table, the target `table`, a `schema_fields` array, and the `write_mode`
/// (`append`, `upsert`, `ignore_conflicts` or `apply_changes`) with its
/// conflict columns.
pub struct MariadbSinkFactory;

impl SinkFactory for MariadbSinkFactory {
    fn type_name(&self) -> &'static str {
        "MariadbSink"
    }

    fn build(
        &self,
        config: &ConfigValue,
        _ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError> {
        let cfg = MariadbSinkConfig::deserialize(config.clone())
            .map_err(|e| SaciError::configuration(format!("MariadbSink config: {e}")))?;
        Ok(Box::new(MariadbSink::new(cfg)?))
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
name "orders_in"
batch_rows 512

connection dsn="mysql://user:pw@db:3306/shop" ssl_mode="disabled"

mode kind="polling" table="shop.orders" cursor_column="id"

schema_fields "id" type="int64" nullable=#false
"#;

    const SINK: &str = r#"
name "orders_out"
table "shop.enriched"
write_mode "upsert"
conflict_columns "id"

connection dsn="mysql://user:pw@db:3306/shop" ssl_mode="disabled"

schema_fields "id" type="int64" nullable=#false
schema_fields "total" type="float64"
"#;

    fn value(text: &str) -> ConfigValue {
        from_kdl_str(text).expect("parse")
    }

    // MariaDB and MySQL carry their own wire format, so neither factory reads a
    // transformer and every build below is handed none.

    #[test]
    fn the_source_factory_builds_from_its_config_table() {
        let source = MariadbSourceFactory
            .build(&value(SOURCE), &ConnectorContext::new(None))
            .expect("source built");
        assert_eq!(MariadbSourceFactory.type_name(), "MariadbSource");
        let schema = source.schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "id");
    }

    #[test]
    fn the_sink_factory_builds_from_its_config_table() {
        let sink = MariadbSinkFactory
            .build(&value(SINK), &ConnectorContext::new(None))
            .expect("sink built");
        assert_eq!(MariadbSinkFactory.type_name(), "MariadbSink");
        let schema = sink.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(1).name(), "total");
    }

    #[test]
    fn a_misspelled_key_is_a_configuration_error_naming_it() {
        // `Box<dyn Source>` is not `Debug`, so the rejection is destructured.
        let Err(err) = MariadbSourceFactory.build(
            &value(&SOURCE.replace("batch_rows", "batch_row")),
            &ConnectorContext::new(None),
        ) else {
            panic!("a misspelled key must be rejected");
        };
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().starts_with("MariadbSource config: "),
            "{}",
            err.message()
        );
        assert!(err.message().contains("batch_row"), "{}", err.message());
    }

    #[test]
    fn a_misspelled_sink_key_is_a_configuration_error_naming_it() {
        let Err(err) = MariadbSinkFactory.build(
            &value(&SINK.replace("write_mode", "write_mod")),
            &ConnectorContext::new(None),
        ) else {
            panic!("a misspelled key must be rejected");
        };
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().starts_with("MariadbSink config: "),
            "{}",
            err.message()
        );
        assert!(err.message().contains("write_mod"), "{}", err.message());
    }

    #[test]
    fn a_failed_validation_reaches_the_caller() {
        let Err(err) = MariadbSinkFactory.build(
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
        assert!(MariadbSourceFactory.rebuildable(&value(SOURCE)).is_ok());
        let reason = MariadbSinkFactory
            .rebuildable(&value(SINK))
            .expect_err("a sink holds buffered rows");
        assert!(reason.contains("buffered"), "{reason}");
    }
}
