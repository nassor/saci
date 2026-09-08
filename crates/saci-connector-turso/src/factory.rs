//! The Turso source and sink factories.
//!
//! Both deserialise the whole `config` sub-table with serde rather than
//! reading [`ConfigValue`] keys by hand: the connector's configuration is a
//! dozen fields across nested tables, and `#[serde(deny_unknown_fields)]` on
//! every one of them is what turns a misspelled key into a startup error
//! instead of a silently ignored setting.
//!
//! [`TursoSource::new`](crate::source::TursoSource::new) and
//! [`TursoSink::new`](crate::sink::TursoSink::new) are synchronous and open no
//! connection, so `saci-service validate` stays database-free; `serve` fails on
//! the first `next_batch`/`write_batch` if the database or endpoint is
//! unreachable.

use serde::Deserialize;

use saci_connector::{ConfigValue, ConnectorContext, SinkFactory, SourceFactory};
use saci_core::error::SaciError;
use saci_core::io::sink::Sink;
use saci_core::io::source::Source;

use crate::{TursoSink, TursoSinkConfig, TursoSource, TursoSourceConfig};

/// Factory for [`TursoSource`].
///
/// The `config` table is [`TursoSourceConfig`]: a `name`, a `connection`
/// table, a `mode` table tagged by `kind` (`polling`, `dump` or `cdc`), and a
/// `schema_fields` array declaring the Arrow schema.
pub struct TursoSourceFactory;

impl SourceFactory for TursoSourceFactory {
    fn type_name(&self) -> &'static str {
        "TursoSource"
    }

    fn build(
        &self,
        config: &ConfigValue,
        _ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        let parsed: TursoSourceConfig = deserialize(config)?;
        Ok(Box::new(TursoSource::new(parsed)?))
    }

    /// The `polling` and `cdc` cursors live in the offset table in the database
    /// and a synced source pulls fresh state, so a fresh reader resumes where
    /// the last committed position left off.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Ok(())
    }
}

/// Factory for [`TursoSink`].
///
/// The `config` table is [`TursoSinkConfig`]: a `name`, a `connection` table,
/// the target `table`, a `schema_fields` array, and the `write_mode`
/// (`append`, `upsert` or `ignore_conflicts`) with its conflict columns.
pub struct TursoSinkFactory;

impl SinkFactory for TursoSinkFactory {
    fn type_name(&self) -> &'static str {
        "TursoSink"
    }

    fn build(
        &self,
        config: &ConfigValue,
        _ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError> {
        let parsed: TursoSinkConfig = deserialize(config)?;
        Ok(Box::new(TursoSink::new(parsed)?))
    }

    // No `rebuildable` override: a sink buffers rows it has accepted but not yet
    // flushed, so replacing the instance would discard them. The trait default
    // refuses it, exactly as the PostgreSQL sink does.
}

/// Deserialise a connector's config table, naming the connector on failure.
fn deserialize<T: for<'de> Deserialize<'de>>(config: &ConfigValue) -> Result<T, SaciError> {
    serde_json::from_value(config.clone())
        .map_err(|e| SaciError::configuration(format!("turso connector config: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use saci_connector::from_kdl_str;

    #[test]
    fn source_factory_builds_without_connecting() {
        let kdl = r#"
            name "orders"
            mode kind="polling" table="orders" cursor_column="id"
            connection path=":memory:"
            schema_fields "id" type="int64" nullable=#false
            schema_fields "total" type="float64"
        "#;
        let config = from_kdl_str(kdl).expect("kdl parses");
        let source = TursoSourceFactory
            .build(&config, &ConnectorContext::new(None))
            .expect("source builds");
        assert_eq!(source.schema().fields().len(), 2);
    }

    #[test]
    fn sink_factory_builds_without_connecting() {
        let kdl = r#"
            name "enriched"
            table "enriched_orders"
            write_mode "upsert"
            conflict_columns "id"
            connection path=":memory:"
            schema_fields "id" type="int64" nullable=#false
            schema_fields "total" type="float64"
        "#;
        let config = from_kdl_str(kdl).expect("kdl parses");
        let sink = TursoSinkFactory
            .build(&config, &ConnectorContext::new(None))
            .expect("sink builds");
        assert_eq!(sink.schema().fields().len(), 2);
    }

    #[test]
    fn a_synced_connection_parses_from_a_remote_block() {
        let kdl = r#"
            name "synced"
            table "orders"
            connection path="replica.db" {
                remote url="libsql://example.turso.io" token="secret"
            }
            schema_fields "id" type="int64" nullable=#false
        "#;
        let config = from_kdl_str(kdl).expect("kdl parses");
        let parsed: TursoSinkConfig = serde_json::from_value(config).expect("config deserializes");
        let remote = parsed.connection.remote.expect("a remote block");
        assert_eq!(remote.url, "libsql://example.turso.io");
        assert_eq!(remote.token, "secret");
    }

    #[test]
    fn a_cdc_source_parses_with_reserved_fields() {
        let kdl = r#"
            name "changes"
            connection path="orders.db"
            mode kind="cdc" table="orders" cdc_table="turso_cdc" retention="delete_acked"
            schema_fields "__op" type="utf8" nullable=#false
            schema_fields "__change_id" type="int64" nullable=#false
            schema_fields "__txn_id" type="int64" nullable=#false
            schema_fields "id" type="int64" nullable=#true
        "#;
        let config = from_kdl_str(kdl).expect("kdl parses");
        let parsed: TursoSourceConfig =
            serde_json::from_value(config).expect("config deserializes");
        assert!(matches!(parsed.mode, crate::config::SourceMode::Cdc(_)));
        assert!(parsed.validate().is_ok());
    }

    fn parse_sink(kdl: &str) -> TursoSinkConfig {
        serde_json::from_value(from_kdl_str(kdl).expect("kdl parses")).expect("config deserializes")
    }

    #[test]
    fn encryption_parses_and_validates() {
        let parsed = parse_sink(
            r#"
            name "sealed"
            table "orders"
            connection path="sealed.db" {
                encryption cipher="aegis256" hexkey="2d7a30108d3eb3e45c90a732041fe54778bdcf707c76749fab7da335d1b39c1d"
            }
            schema_fields "id" type="int64" nullable=#false
        "#,
        );
        let encryption = parsed
            .connection
            .encryption
            .as_ref()
            .expect("an encryption block");
        assert_eq!(encryption.cipher, "aegis256");
        assert!(parsed.validate().is_ok());
    }

    #[test]
    fn a_short_encryption_key_is_rejected() {
        let parsed = parse_sink(
            r#"
            name "sealed"
            table "orders"
            connection path="sealed.db" {
                encryption cipher="aes128gcm" hexkey="abcd"
            }
            schema_fields "id" type="int64" nullable=#false
        "#,
        );
        let error = parsed.validate().expect_err("a 4-digit key is too short");
        assert!(error.message().contains("hexkey"), "{}", error.message());
    }

    #[test]
    fn encryption_on_a_synced_connection_is_rejected() {
        let parsed = parse_sink(
            r#"
            name "sealed"
            table "orders"
            connection path="replica.db" {
                encryption cipher="aes128gcm" hexkey="5f3e2a8c9b1d4f6e7a2c8d4b9e1f3a6c"
                remote url="libsql://example.turso.io" token="secret"
            }
            schema_fields "id" type="int64" nullable=#false
        "#,
        );
        let error = parsed
            .validate()
            .expect_err("a replica takes no local encryption");
        assert!(
            error.message().contains("embedded database only"),
            "{}",
            error.message()
        );
    }

    #[test]
    fn concurrent_with_capture_is_rejected() {
        let parsed = parse_sink(
            r#"
            name "concurrent"
            table "orders"
            transaction "concurrent"
            connection path="orders.db"
            capture mode="full"
            schema_fields "id" type="int64" nullable=#false
        "#,
        );
        let error = parsed.validate().expect_err("MVCC and CDC are exclusive");
        assert!(
            error.message().contains("mutually exclusive"),
            "{}",
            error.message()
        );
    }

    #[test]
    fn a_schema_field_name_with_sql_metacharacters_is_rejected() {
        let parsed = parse_sink(
            r#"
            name "orders"
            table "orders"
            connection path="orders.db"
            schema_fields "bad name; DROP TABLE orders; --" type="utf8"
        "#,
        );
        let error = parsed
            .validate()
            .expect_err("a column name reaches SQL unquoted");
        assert!(
            error.message().contains("schema_fields id"),
            "{}",
            error.message()
        );
    }
}
