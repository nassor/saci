//! The file [`Connector`].
//!
//! Both roles resolve their byte format through the codec the host bound to
//! this connector instance via a declared `codec` key. The schema is
//! where they differ: a source hands the format whatever `field` entries
//! says, including nothing, and lets the format decide, while a sink always
//! needs one because it is the schema the rows are written with.

use crate::connector::schema::deserialize_config;
use crate::connector::{
    ConfigValue, Connector, ConnectorContext, Rebuild, Role, parse_fields, parse_optional_fields,
    schema_of,
};
use crate::error::SaciResult;
use crate::io::{Sink, Source};

use crate::connector::file::{FileSink, FileSinkConfig, FileSource, FileSourceConfig};

/// The local-file connector: a `file` node of either role.
///
/// The source `config` table is [`FileSourceConfig`]:
/// - `path` (string, required): the file to read.
///
/// Beside it, `field` entries (optional) declare the Arrow schema.
/// Required by `csv`, a projection target for `parquet`, `avro` and
/// `arrow-ipc`, and inferred by `ndjson` when absent.
///
/// The sink `config` table is [`FileSinkConfig`]:
/// - `path` (string, required): the file to write.
/// - `truncate` (bool, optional, default `false`): replace the file rather
///   than append to it.
///
/// Beside it, `field` entries (at least one, required) declare the Arrow
/// schema for the output file.
///
/// The byte format is whatever codec the node's `codec` key names; see
/// [`ConnectorContext::codec`].
pub struct FileConnector;

impl Connector for FileConnector {
    fn name(&self) -> &'static str {
        super::NAME
    }

    fn source(&self, config: &ConfigValue, ctx: &ConnectorContext) -> SaciResult<Box<dyn Source>> {
        let cfg: FileSourceConfig = deserialize_config(config, "FileSource")?;
        let codec = ctx.codec("FileSource")?;
        let declared = parse_optional_fields(config, "FileSource")?
            .map(|fields| schema_of(&fields))
            .transpose()?;
        Ok(Box::new(FileSource::new_with_corrupt(
            cfg,
            declared,
            codec,
            ctx.skip_corrupt(),
        )?))
    }

    fn sink(&self, config: &ConfigValue, ctx: &ConnectorContext) -> SaciResult<Box<dyn Sink>> {
        let cfg: FileSinkConfig = deserialize_config(config, "FileSink")?;
        let codec = ctx.codec("FileSink")?;
        let schema = schema_of(&parse_fields(config, "FileSink")?)?;
        Ok(Box::new(FileSink::new(cfg, schema, codec)?))
    }

    /// A file is read from its start, so a second source build re-delivers
    /// every row the first one already handed over. An appending sink
    /// reopened is an appending sink; a truncating one would replace the file
    /// and erase what this run already wrote.
    fn rebuild(&self, role: Role, config: &ConfigValue) -> Rebuild {
        match role {
            Role::Source => Rebuild::No(
                "a file is read from its start, so a second build re-delivers every row already read",
            ),
            Role::Sink => {
                let truncate = config
                    .get("truncate")
                    .and_then(ConfigValue::as_bool)
                    .unwrap_or(false);
                if truncate {
                    Rebuild::No(
                        "truncate #true replaces the file, so a second build would erase the rows \
                         this run already wrote",
                    )
                } else {
                    Rebuild::Yes
                }
            }
        }
    }

    fn detail_keys(&self, _role: Role) -> &'static [&'static str] {
        &["path"]
    }
}

#[cfg(test)]
mod connector_tests {
    use super::*;
    use crate::connector::{ConfigMap, from_kdl_str};

    fn empty_config() -> ConfigValue {
        ConfigValue::Object(ConfigMap::new())
    }

    #[test]
    fn the_connector_name_matches_the_config_connector_key() {
        assert_eq!(FileConnector.name(), "file");
        assert_eq!(FileConnector.roles(), &[Role::Source, Role::Sink]);
        assert_eq!(FileConnector.detail_keys(Role::Source), &["path"]);
        assert_eq!(FileConnector.detail_keys(Role::Sink), &["path"]);
        assert!(!FileConnector.live(&empty_config()));
    }

    #[test]
    fn only_an_appending_sink_is_rebuildable() {
        assert!(matches!(
            FileConnector.rebuild(Role::Source, &empty_config()),
            Rebuild::No(_)
        ));
        assert_eq!(
            FileConnector.rebuild(Role::Sink, &empty_config()),
            Rebuild::Yes
        );
        let truncating = from_kdl_str("truncate #true\n").expect("parse test config");
        assert!(matches!(
            FileConnector.rebuild(Role::Sink, &truncating),
            Rebuild::No(_)
        ));
    }
}

#[cfg(all(
    test,
    feature = "codec-csv",
    feature = "codec-ndjson",
    feature = "codec-parquet"
))]
mod tests {
    use std::sync::Arc;

    use crate::codec::csv::CsvCodecFactory;
    use crate::codec::ndjson::NdjsonCodecFactory;
    use crate::codec::parquet::ParquetCodecFactory;
    use crate::codec::{Codec, CodecFactory};
    use crate::connector::{ConfigMap, from_kdl_str};
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use tempfile::TempDir;

    use super::*;

    fn csv_codec(options: &ConfigValue) -> Arc<dyn Codec> {
        CsvCodecFactory.build(options).expect("csv codec builds")
    }

    fn parquet_codec() -> Arc<dyn Codec> {
        ParquetCodecFactory
            .build(&ConfigValue::Object(ConfigMap::new()))
            .expect("parquet codec builds")
    }

    fn ndjson_codec() -> Arc<dyn Codec> {
        NdjsonCodecFactory
            .build(&ConfigValue::Object(ConfigMap::new()))
            .expect("ndjson codec builds")
    }

    fn empty_config() -> ConfigValue {
        ConfigValue::Object(ConfigMap::new())
    }

    /// A path is written into a config string, and a Windows path has
    /// backslashes a KDL quoted string reads as escapes.
    fn config_path(dir: &TempDir, name: &str) -> String {
        dir.path().join(name).to_string_lossy().replace('\\', "/")
    }

    fn config(raw: &str) -> ConfigValue {
        from_kdl_str(raw).expect("parse test config")
    }

    const CSV_SCHEMA: &str = r#"
field "id" type="int64" nullable=#false
"#;

    #[test]
    fn a_missing_path_is_a_configuration_error() {
        let ctx = ConnectorContext::new(None);

        let Err(err) = FileConnector.source(&empty_config(), &ctx) else {
            panic!("path is required");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("FileSource config"), "got: {err}");
        assert!(err.message().contains("`path`"), "got: {err}");

        let Err(err) = FileConnector.sink(&empty_config(), &ctx) else {
            panic!("path is required");
        };
        assert!(err.message().contains("FileSink config"), "got: {err}");
        assert!(err.message().contains("`path`"), "got: {err}");
    }

    /// A misspelled key is refused and named, not ignored: the source would
    /// otherwise read a different file than the config says.
    #[test]
    fn a_misspelled_key_is_a_configuration_error_naming_it() {
        let ctx = ConnectorContext::new(Some(ndjson_codec()));
        let raw = format!("pth \"/data/in.ndjson\"\n{CSV_SCHEMA}");

        let Err(err) = FileConnector.source(&config(&raw), &ctx) else {
            panic!("`pth` is not a file source key");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("pth"), "got: {err}");

        let raw = format!("path \"/data/out.ndjson\"\ntruncat #true\n{CSV_SCHEMA}");
        let Err(err) = FileConnector.sink(&config(&raw), &ctx) else {
            panic!("`truncat` is not a file sink key");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("truncat"), "got: {err}");
    }

    #[test]
    fn a_source_with_no_bound_codec_is_a_configuration_error() {
        let ctx = ConnectorContext::new(None);
        let dir = TempDir::new().expect("temp dir");
        let raw = format!("path \"{}\"\n{CSV_SCHEMA}", config_path(&dir, "in.csv"));

        let Err(err) = FileConnector.source(&config(&raw), &ctx) else {
            panic!("a source that moves bytes needs a bound codec");
        };
        assert_eq!(
            err.message(),
            "FileSource moves bytes and needs a 'codec' key naming a declared codec"
        );
    }

    #[test]
    fn parquet_with_declared_fields_projects_onto_them() {
        let ctx = ConnectorContext::new(Some(parquet_codec()));
        let dir = TempDir::new().expect("temp dir");

        // A two-column file, so the declaration has a column to drop. Parquet's
        // message surface is one whole file per payload, so one encode is the
        // fixture.
        let file_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("extra", DataType::Int64, false),
        ]));
        let file_batch = RecordBatch::try_new(
            Arc::clone(&file_schema),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2])),
                Arc::new(Int64Array::from(vec![3i64, 4])),
            ],
        )
        .expect("batch");
        let payloads = parquet_codec()
            .encode_messages(&file_batch)
            .expect("encode");
        std::fs::write(dir.path().join("in.parquet"), &payloads[0]).expect("fixture");

        let raw = format!("path \"{}\"\n{CSV_SCHEMA}", config_path(&dir, "in.parquet"));
        let source = FileConnector
            .source(&config(&raw), &ctx)
            .expect("a declared schema is a projection target");
        assert_eq!(source.schema().fields().len(), 1);
        assert_eq!(source.schema().field(0).name(), "id");
    }

    #[test]
    fn a_sink_without_fields_is_a_configuration_error() {
        let ctx = ConnectorContext::new(Some(csv_codec(&empty_config())));
        let dir = TempDir::new().expect("temp dir");
        let raw = format!("path \"{}\"\n", config_path(&dir, "out.csv"));

        let Err(err) = FileConnector.sink(&config(&raw), &ctx) else {
            panic!("a sink needs the schema it writes");
        };
        assert!(err.message().contains("'field'"), "got: {err}");
    }

    #[test]
    fn a_csv_source_builds_and_reports_the_declared_schema() {
        let mut options = ConfigMap::new();
        options.insert("has_headers".to_string(), ConfigValue::Bool(true));
        let ctx = ConnectorContext::new(Some(csv_codec(&ConfigValue::Object(options))));
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("in.csv");
        std::fs::write(&path, "id\n1\n2\n").expect("fixture");

        let raw = format!("path \"{}\"\n{CSV_SCHEMA}", config_path(&dir, "in.csv"));
        let source = FileConnector
            .source(&config(&raw), &ctx)
            .expect("source builds");
        assert_eq!(source.schema().fields().len(), 1);
        assert_eq!(source.schema().field(0).name(), "id");
    }

    /// Build a sink through the connector and write one row holding `id`.
    /// `extra` carries the config lines under test.
    async fn write_one_row(dir: &TempDir, name: &str, extra: &str, id: i64) {
        let ctx = ConnectorContext::new(Some(ndjson_codec()));
        let raw = format!("path \"{}\"\n{CSV_SCHEMA}{extra}", config_path(dir, name));
        let mut sink = FileConnector
            .sink(&config(&raw), &ctx)
            .expect("sink builds");

        let batch = RecordBatch::try_new(sink.schema(), vec![Arc::new(Int64Array::from(vec![id]))])
            .expect("batch");
        sink.write_batch(&batch).await.expect("write");
        sink.finish().await.expect("finish");
    }

    fn lines(dir: &TempDir, name: &str) -> Vec<String> {
        std::fs::read_to_string(dir.path().join(name))
            .expect("output file")
            .lines()
            .map(str::to_string)
            .collect()
    }

    #[tokio::test]
    async fn the_truncate_key_picks_the_sink_open_mode() {
        let dir = TempDir::new().expect("temp dir");

        // Absent: the second sink appends, so both runs' rows survive.
        write_one_row(&dir, "append.ndjson", "", 1).await;
        write_one_row(&dir, "append.ndjson", "", 2).await;
        let appended = lines(&dir, "append.ndjson");
        assert_eq!(appended.len(), 2, "got: {appended:?}");
        assert!(appended[0].contains('1'), "got: {appended:?}");
        assert!(appended[1].contains('2'), "got: {appended:?}");

        // Set: the second sink replaces the file, so only its row is left.
        write_one_row(&dir, "replace.ndjson", "truncate #true\n", 1).await;
        write_one_row(&dir, "replace.ndjson", "truncate #true\n", 2).await;
        let replaced = lines(&dir, "replace.ndjson");
        assert_eq!(replaced.len(), 1, "got: {replaced:?}");
        assert!(replaced[0].contains('2'), "got: {replaced:?}");
    }
}
