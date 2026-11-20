//! The file source and sink factories.
//!
//! Both resolve their byte format through the codec the host bound to
//! this connector instance via a declared `transformer` key. The schema is
//! where they differ: a source hands the format whatever `schema_fields`
//! says, including nothing, and lets the format decide, while a sink always
//! needs one because it is the schema the rows are written with.

use crate::connector::schema::deserialize_config;
use crate::connector::{
    ConfigValue, ConnectorContext, SinkFactory, SourceFactory, parse_optional_schema_fields,
    parse_schema_fields,
};
use crate::error::SaciError;
use crate::io::{Sink, Source};

use crate::connector::file::{FileSink, FileSinkConfig, FileSource, FileSourceConfig};

/// Factory for [`FileSource`].
///
/// The `config` table is [`FileSourceConfig`]:
/// - `path` (string, required): the file to read.
///
/// Beside it, `schema_fields` (list, optional) is the declared Arrow schema.
/// Required by `csv`, a projection target for `parquet`, `avro` and
/// `arrow-ipc`, and inferred by `ndjson` when absent.
///
/// The byte format is whatever codec the `source` node's `transformer`
/// key names; see [`ConnectorContext::codec`].
pub struct FileSourceFactory;

impl SourceFactory for FileSourceFactory {
    fn type_name(&self) -> &'static str {
        "FileSource"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        let cfg: FileSourceConfig = deserialize_config(config, "FileSource")?;
        let codec = ctx.codec("FileSource")?;
        let declared = parse_optional_schema_fields(config, "FileSource")?;
        Ok(Box::new(FileSource::new(cfg, declared, codec)?))
    }

    /// A file is read from its start, so a second build re-delivers every
    /// row the first one already handed over.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Err("a file is read from its start, so a second build re-delivers every row already read")
    }
}

/// Factory for [`FileSink`].
///
/// The `config` table is [`FileSinkConfig`]:
/// - `path` (string, required): the file to write.
/// - `truncate` (bool, optional, default `false`): replace the file rather
///   than append to it.
///
/// Beside it, `schema_fields` (list, required) is the Arrow schema for the
/// output file.
///
/// The byte format is whatever codec the `sink` node's `transformer`
/// key names; see [`ConnectorContext::codec`].
pub struct FileSinkFactory;

impl SinkFactory for FileSinkFactory {
    fn type_name(&self) -> &'static str {
        "FileSink"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError> {
        let cfg: FileSinkConfig = deserialize_config(config, "FileSink")?;
        let codec = ctx.codec("FileSink")?;
        let schema = parse_schema_fields(config, "FileSink")?;
        Ok(Box::new(FileSink::new(cfg, schema, codec)?))
    }

    /// An appending sink reopened is an appending sink; a truncating one
    /// would replace the file and erase what this run already wrote.
    fn rebuildable(&self, config: &ConfigValue) -> Result<(), &'static str> {
        let truncate = config
            .get("truncate")
            .and_then(ConfigValue::as_bool)
            .unwrap_or(false);
        if truncate {
            return Err(
                "truncate #true replaces the file, so a second build would erase the rows \
                 this run already wrote",
            );
        }
        Ok(())
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
schema_fields "id" type="Int64" nullable=#false
"#;

    #[test]
    fn the_type_names_match_the_config_type_key() {
        assert_eq!(FileSourceFactory.type_name(), "FileSource");
        assert_eq!(FileSinkFactory.type_name(), "FileSink");
    }

    #[test]
    fn a_missing_path_is_a_configuration_error() {
        let ctx = ConnectorContext::new(None);

        let Err(err) = FileSourceFactory.build(&empty_config(), &ctx) else {
            panic!("path is required");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("FileSource config"), "got: {err}");
        assert!(err.message().contains("`path`"), "got: {err}");

        let Err(err) = FileSinkFactory.build(&empty_config(), &ctx) else {
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

        let Err(err) = FileSourceFactory.build(&config(&raw), &ctx) else {
            panic!("`pth` is not a file source key");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("pth"), "got: {err}");

        let raw = format!("path \"/data/out.ndjson\"\ntruncat #true\n{CSV_SCHEMA}");
        let Err(err) = FileSinkFactory.build(&config(&raw), &ctx) else {
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

        let Err(err) = FileSourceFactory.build(&config(&raw), &ctx) else {
            panic!("a source that moves bytes needs a bound codec");
        };
        assert_eq!(
            err.message(),
            "FileSource moves bytes and needs a 'transformer' key naming a declared transformer"
        );
    }

    #[test]
    fn parquet_with_declared_schema_fields_projects_onto_them() {
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
        let source = FileSourceFactory
            .build(&config(&raw), &ctx)
            .expect("a declared schema is a projection target");
        assert_eq!(source.schema().fields().len(), 1);
        assert_eq!(source.schema().field(0).name(), "id");
    }

    #[test]
    fn a_sink_without_schema_fields_is_a_configuration_error() {
        let ctx = ConnectorContext::new(Some(csv_codec(&empty_config())));
        let dir = TempDir::new().expect("temp dir");
        let raw = format!("path \"{}\"\n", config_path(&dir, "out.csv"));

        let Err(err) = FileSinkFactory.build(&config(&raw), &ctx) else {
            panic!("a sink needs the schema it writes");
        };
        assert!(err.message().contains("schema_fields"), "got: {err}");
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
        let source = FileSourceFactory
            .build(&config(&raw), &ctx)
            .expect("source builds");
        assert_eq!(source.schema().fields().len(), 1);
        assert_eq!(source.schema().field(0).name(), "id");
    }

    /// Build a sink through the factory and write one row holding `id`.
    /// `extra` carries the config lines under test.
    async fn write_one_row(dir: &TempDir, name: &str, extra: &str, id: i64) {
        let ctx = ConnectorContext::new(Some(ndjson_codec()));
        let raw = format!("path \"{}\"\n{CSV_SCHEMA}{extra}", config_path(dir, name));
        let mut sink = FileSinkFactory
            .build(&config(&raw), &ctx)
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
