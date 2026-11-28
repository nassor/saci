//! The redb [`Connector`].
//!
//! Both roles resolve their byte format through the codec the host bound to
//! this connector instance via a declared `codec` key. Both need the
//! declared `field` entries: a source's schema is read at load time by
//! `validate_workflow_graph`, before the file is opened, so it cannot be
//! discovered from the table, and a sink writes with it.

use serde::Deserialize;

use crate::connector::schema::without_fields;
use crate::connector::{
    ConfigValue, Connector, ConnectorContext, Rebuild, Role, parse_fields, schema_of,
};
use crate::error::{SaciError, SaciResult};
use crate::io::{Sink, Source};

use crate::connector::redb::config::{RedbSinkConfig, RedbSourceConfig};
use crate::connector::redb::{RedbSink, RedbSource};

/// The embedded redb connector: a `redb` node of either role.
///
/// Source config fields ([`RedbSource`]):
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
/// - `field` (one or more, required): the declared Arrow schema.
///
/// redb gives the source's read-only handle a shared OS lock, so several
/// sources may read one file at once. A sink on the same path still cannot
/// run at the same time: its write handle is exclusive. A read-only open also
/// never repairs, so a file whose last write left no allocator state table is
/// refused with a message naming `check_integrity`; the sink's
/// `quick_repair`/`two_phase_commit` defaults keep every file it writes out
/// of that state.
///
/// Sink config fields ([`RedbSink`]):
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
/// - `field` (one or more, required): the Arrow schema the rows are written
///   with.
///
/// A sink locks the file exclusively from the build until `finish`, so no
/// other handle, a source included, can open the same path at the same time.
///
/// The byte format is whatever codec the node's `codec` key names; see
/// [`ConnectorContext::codec`].
pub struct RedbConnector;

impl Connector for RedbConnector {
    fn name(&self) -> &'static str {
        super::NAME
    }

    fn source(&self, config: &ConfigValue, ctx: &ConnectorContext) -> SaciResult<Box<dyn Source>> {
        let cfg = RedbSourceConfig::deserialize(without_fields(config))
            .map_err(|e| SaciError::configuration(format!("RedbSource config: {e}")))?;
        let codec = ctx.codec("RedbSource")?;
        let schema = schema_of(&parse_fields(config, "RedbSource")?)?;
        let mut src = RedbSource::new(cfg, schema, codec)?;
        src.set_skip_corrupt(ctx.skip_corrupt());
        Ok(Box::new(src))
    }

    fn sink(&self, config: &ConfigValue, ctx: &ConnectorContext) -> SaciResult<Box<dyn Sink>> {
        let cfg = RedbSinkConfig::deserialize(without_fields(config))
            .map_err(|e| SaciError::configuration(format!("RedbSink config: {e}")))?;
        let codec = ctx.codec("RedbSink")?;
        let schema = schema_of(&parse_fields(config, "RedbSink")?)?;
        Ok(Box::new(RedbSink::new(cfg, schema, codec)?))
    }

    /// The source's table is scanned once per instance, so a fresh one
    /// re-delivers every entry already read. Every batch a sink accepts is
    /// its own committed transaction, so nothing is buffered for a rebuild to
    /// discard; the host drops the failed instance before it builds the
    /// replacement, which releases the file lock, and the fresh instance
    /// resumes the key sequence from the file itself.
    fn rebuild(&self, role: Role, _config: &ConfigValue) -> Rebuild {
        match role {
            Role::Source => Rebuild::No(
                "the table is listed once, so a second build re-delivers every entry \
                 already read",
            ),
            Role::Sink => Rebuild::Yes,
        }
    }

    fn detail_keys(&self, role: Role) -> &'static [&'static str] {
        match role {
            Role::Source => &["directory", "file", "table", "key_prefix", "key_suffix"],
            Role::Sink => &[
                "directory",
                "file",
                "table",
                "key_prefix",
                "key_suffix",
                "durability",
                "compact",
            ],
        }
    }
}

#[cfg(test)]
mod connector_tests {
    use super::*;
    use crate::connector::ConfigMap;

    fn empty_config() -> ConfigValue {
        ConfigValue::Object(ConfigMap::new())
    }

    #[test]
    fn the_connector_name_roles_and_detail_keys() {
        assert_eq!(RedbConnector.name(), "redb");
        assert_eq!(RedbConnector.roles(), &[Role::Source, Role::Sink]);
        assert_eq!(
            RedbConnector.detail_keys(Role::Source),
            &["directory", "file", "table", "key_prefix", "key_suffix"]
        );
        assert_eq!(
            RedbConnector.detail_keys(Role::Sink),
            &[
                "directory",
                "file",
                "table",
                "key_prefix",
                "key_suffix",
                "durability",
                "compact"
            ]
        );
        assert!(!RedbConnector.live(&empty_config()));
    }

    #[test]
    fn the_sink_is_rebuildable_and_the_source_is_not() {
        let config = empty_config();
        assert_eq!(RedbConnector.rebuild(Role::Sink, &config), Rebuild::Yes);
        assert!(matches!(
            RedbConnector.rebuild(Role::Source, &config),
            Rebuild::No(_)
        ));
    }
}

#[cfg(all(test, feature = "codec-csv"))]
mod tests {
    use std::sync::Arc;

    use crate::codec::csv::CsvCodecFactory;
    use crate::codec::{Codec, CodecFactory};
    use crate::connector::{ConfigMap, from_kdl_str};

    use super::*;

    fn empty_config() -> ConfigValue {
        ConfigValue::Object(ConfigMap::new())
    }

    fn config(raw: &str) -> ConfigValue {
        from_kdl_str(raw).expect("parse test config")
    }

    fn csv_codec() -> Arc<dyn Codec> {
        CsvCodecFactory
            .build(&empty_config())
            .expect("csv codec builds")
    }

    const SCHEMA: &str = r#"
field "id" type="int64" nullable=#false
"#;

    #[test]
    fn a_missing_directory_is_a_configuration_error() {
        let config = config(SCHEMA);
        let Err(err) = RedbConnector.source(&config, &ConnectorContext::new(Some(csv_codec())))
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
        let Err(err) = RedbConnector.sink(&config, &ConnectorContext::new(Some(csv_codec())))
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
        let Err(err) = RedbConnector.sink(&config, &ConnectorContext::new(Some(csv_codec())))
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
    fn a_missing_codec_names_the_connector() {
        let config = config(&format!(
            r#"directory "/tmp/saci-redb-factory-test"
{SCHEMA}"#
        ));
        let Err(err) = RedbConnector.source(&config, &ConnectorContext::new(None)) else {
            panic!("no codec bound must fail");
        };
        assert_eq!(err.category(), "configuration");
        assert_eq!(
            err.message(),
            "RedbSource moves bytes and needs a 'codec' key naming a declared codec"
        );
    }

    #[test]
    fn a_missing_field_is_a_configuration_error() {
        let config = config(r#"directory "/tmp/saci-redb-factory-test""#);
        let Err(err) = RedbConnector.source(&config, &ConnectorContext::new(Some(csv_codec())))
        else {
            panic!("missing schema must fail");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'field'"), "got: {err}");
    }

    #[test]
    fn the_source_builds_without_touching_the_file() {
        let config = config(&format!(
            r#"directory "/tmp/saci-redb-no-such-directory"
{SCHEMA}"#
        ));
        RedbConnector
            .source(&config, &ConnectorContext::new(Some(csv_codec())))
            .expect("source builds without opening the file");
    }
}
