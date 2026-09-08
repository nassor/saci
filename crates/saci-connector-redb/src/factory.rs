//! The redb source and sink factories.
//!
//! Both resolve their byte format through the transformer the host bound to
//! this connector instance via a declared `transformer` key. Both need the
//! declared `schema_fields`: a source's schema is read at load time by
//! `validate_workflow_graph`, before the file is opened, so it cannot be
//! discovered from the table, and a sink writes with it.

use serde::Deserialize;

use saci_connector::{
    ConfigValue, ConnectorContext, SinkFactory, SourceFactory, parse_schema_fields,
};
use saci_core::error::SaciError;
use saci_core::io::{sink::Sink, source::Source};

use crate::config::{RedbSinkConfig, RedbSourceConfig};
use crate::{RedbSink, RedbSource};

/// Factory for [`RedbSource`].
///
/// Config fields:
/// - `directory` (string, required): directory the redb file lives in.
/// - `file` (string, optional): file name inside it, `saci.redb` by default.
/// - `table` (string, optional): table name inside the file, `records` by
///   default.
/// - `key_prefix` (string, optional): only entries whose key starts with it
///   are read.
/// - `key_suffix` (string, optional): only entries whose key ends with it are
///   read.
/// - `check_integrity` (bool, optional): walk the whole file at open, off by
///   default; also the way to repair a file a killed sink left unclean, since
///   a read-only open cannot.
/// - `cache_size_bytes` (int, optional): page cache budget; redb's own default
///   when absent.
/// - `schema_fields` (list, required): the declared Arrow schema.
///
/// redb gives the source's read-only handle a shared OS lock, so several
/// sources may read one file at once. A `RedbSink` on the same path still
/// cannot run at the same time: its write handle is exclusive. A read-only
/// open also never repairs, so a file whose last write left no allocator
/// state table is refused with a message naming `check_integrity`; the sink's
/// `quick_repair`/`two_phase_commit` defaults keep every file it writes out
/// of that state.
///
/// The byte format is whatever transformer the `source` node's `transformer`
/// key names; see [`ConnectorContext::transformer`].
pub struct RedbSourceFactory;

impl SourceFactory for RedbSourceFactory {
    fn type_name(&self) -> &'static str {
        "RedbSource"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        let cfg = RedbSourceConfig::deserialize(config.clone())
            .map_err(|e| SaciError::configuration(format!("RedbSource config: {e}")))?;
        let transformer = ctx.transformer("RedbSource")?;
        let schema = parse_schema_fields(config, "RedbSource")?;
        Ok(Box::new(RedbSource::new(cfg, schema, transformer)?))
    }

    /// The table is scanned once per instance, so a fresh one re-delivers
    /// every entry already read.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Err(
            "the table is listed once, so a second build re-delivers every entry \
             already read",
        )
    }
}

/// Factory for [`RedbSink`].
///
/// Config fields:
/// - `directory` (string, required): directory the redb file lives in,
///   created if absent.
/// - `file` (string, optional): file name inside it, `saci.redb` by default.
/// - `table` (string, optional): table name inside the file, `records` by
///   default.
/// - `key_prefix` (string, optional): prepended to every generated key.
/// - `key_suffix` (string, optional): appended to every generated key.
/// - `check_integrity` (bool, optional): walk the whole file at open, off by
///   default.
/// - `cache_size_bytes` (int, optional): page cache budget; redb's own default
///   when absent.
/// - `compact` (bool, optional): reclaim free space at `finish`, on by
///   default.
/// - `durability` (`"immediate"` or `"none"`, optional): how hard each commit
///   tries, `immediate` by default.
/// - `two_phase_commit` (bool, optional): on by default.
/// - `quick_repair` (bool, optional): on by default.
/// - `schema_fields` (list, required): the Arrow schema the rows are written
///   with.
///
/// redb locks the file exclusively from the build until `finish`, so no other
/// handle, `RedbSource` included, can open the same path at the same time.
///
/// The byte format is whatever transformer the `sink` node's `transformer` key
/// names; see [`ConnectorContext::transformer`].
pub struct RedbSinkFactory;

impl SinkFactory for RedbSinkFactory {
    fn type_name(&self) -> &'static str {
        "RedbSink"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError> {
        let cfg = RedbSinkConfig::deserialize(config.clone())
            .map_err(|e| SaciError::configuration(format!("RedbSink config: {e}")))?;
        let transformer = ctx.transformer("RedbSink")?;
        let schema = parse_schema_fields(config, "RedbSink")?;
        Ok(Box::new(RedbSink::open(cfg, schema, transformer)?))
    }

    /// Every accepted batch is its own committed transaction, so nothing is
    /// buffered for a rebuild to discard. The host drops the failed instance
    /// before it builds the replacement, which releases the file lock, and the
    /// fresh instance resumes the key sequence from the file itself.
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

    const SCHEMA: &str = r#"
schema_fields "id" type="Int64" nullable=#false
"#;

    #[test]
    fn the_type_names_match_the_config_type_key() {
        assert_eq!(RedbSourceFactory.type_name(), "RedbSource");
        assert_eq!(RedbSinkFactory.type_name(), "RedbSink");
    }

    #[test]
    fn a_missing_directory_is_a_configuration_error() {
        let config = config(SCHEMA);
        let Err(err) =
            RedbSourceFactory.build(&config, &ConnectorContext::new(Some(csv_transformer())))
        else {
            panic!("missing directory must fail");
        };
        assert_eq!(err.category(), "configuration");
        assert_eq!(
            err.message(),
            "RedbSource config: missing field `directory`"
        );
    }

    #[test]
    fn an_unknown_key_is_refused_by_name() {
        let config = config(&format!(
            r#"directory "/tmp/saci-redb-factory-test"
bogus_key "x"
{SCHEMA}"#
        ));
        let Err(err) =
            RedbSinkFactory.build(&config, &ConnectorContext::new(Some(csv_transformer())))
        else {
            panic!("an unknown key must fail");
        };
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("bogus_key"),
            "message was {}",
            err.message()
        );
    }

    #[test]
    fn an_unknown_durability_level_names_the_two_that_exist() {
        let config = config(&format!(
            r#"directory "/tmp/saci-redb-factory-test"
durability "bogus"
{SCHEMA}"#
        ));
        let Err(err) =
            RedbSinkFactory.build(&config, &ConnectorContext::new(Some(csv_transformer())))
        else {
            panic!("an unknown durability level must fail");
        };
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("expected `immediate` or `none`"),
            "message was {}",
            err.message()
        );
    }

    #[test]
    fn a_missing_transformer_names_the_connector() {
        let config = config(&format!(
            r#"directory "/tmp/saci-redb-factory-test"
{SCHEMA}"#
        ));
        let Err(err) = RedbSourceFactory.build(&config, &ConnectorContext::new(None)) else {
            panic!("no transformer bound must fail");
        };
        assert_eq!(err.category(), "configuration");
        assert_eq!(
            err.message(),
            "RedbSource moves bytes and needs a 'transformer' key naming a declared transformer"
        );
    }

    #[test]
    fn a_missing_schema_fields_is_a_configuration_error() {
        let config = config(r#"directory "/tmp/saci-redb-factory-test""#);
        let Err(err) =
            RedbSourceFactory.build(&config, &ConnectorContext::new(Some(csv_transformer())))
        else {
            panic!("missing schema must fail");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("schema_fields"));
    }

    #[test]
    fn the_source_builds_without_touching_the_file() {
        let config = config(&format!(
            r#"directory "/tmp/saci-redb-no-such-directory"
{SCHEMA}"#
        ));
        RedbSourceFactory
            .build(&config, &ConnectorContext::new(Some(csv_transformer())))
            .expect("source builds without opening the file");
    }

    #[test]
    fn the_sink_is_rebuildable_and_the_source_is_not() {
        let config = empty_config();
        assert!(RedbSinkFactory.rebuildable(&config).is_ok());
        assert!(RedbSourceFactory.rebuildable(&config).is_err());
    }
}
