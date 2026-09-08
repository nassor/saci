//! The file source and sink factories.
//!
//! Both resolve their byte format through the transformer the host bound to
//! this connector instance via a declared `transformer` key. The schema is
//! where they differ: a source hands the format whatever `schema_fields`
//! says, including nothing, and lets the format decide, while a sink always
//! needs one because it is the schema the rows are written with.

use std::path::Path;

use saci_connector::{
    ConfigValue, ConnectorContext, SinkFactory, SourceFactory, parse_optional_schema_fields,
    parse_schema_fields,
};
use saci_core::error::SaciError;
use saci_core::io::{sink::Sink, source::Source};

use crate::{FileSink, FileSource};

/// Factory for [`FileSource`].
///
/// Config fields:
/// - `path` (string, required): the file to read.
/// - `schema_fields` (list, optional): the declared Arrow schema. Required by
///   `csv`, a projection target for `parquet`, `avro` and `arrow-ipc`, and
///   inferred by `ndjson` when absent.
///
/// The byte format is whatever transformer the `source` node's `transformer`
/// key names; see [`ConnectorContext::transformer`].
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
        let path = config.get("path").and_then(|v| v.as_str()).ok_or_else(|| {
            SaciError::configuration("FileSource config requires a 'path' string field")
        })?;
        let transformer = ctx.transformer("FileSource")?;
        let declared = parse_optional_schema_fields(config, "FileSource")?;
        Ok(Box::new(FileSource::open(
            Path::new(path),
            transformer,
            declared,
        )?))
    }

    /// A file is read from its start, so a second build re-delivers every
    /// row the first one already handed over.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Err("a file is read from its start, so a second build re-delivers every row already read")
    }
}

/// Factory for [`FileSink`].
///
/// Config fields:
/// - `path` (string, required): the file to write.
/// - `schema_fields` (list, required): the Arrow schema for the output file.
/// - `truncate` (bool, optional, default `false`): replace the file rather
///   than append to it.
///
/// The byte format is whatever transformer the `sink` node's `transformer`
/// key names; see [`ConnectorContext::transformer`].
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
        let path = config.get("path").and_then(|v| v.as_str()).ok_or_else(|| {
            SaciError::configuration("FileSink config requires a 'path' string field")
        })?;
        let transformer = ctx.transformer("FileSink")?;
        let schema = parse_schema_fields(config, "FileSink")?;
        let truncate = config
            .get("truncate")
            .and_then(ConfigValue::as_bool)
            .unwrap_or(false);
        let path = Path::new(path);
        let sink = if truncate {
            FileSink::create_truncating(path, transformer, schema)?
        } else {
            FileSink::create(path, transformer, schema)?
        };
        Ok(Box::new(sink))
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use saci_connector::{ConfigMap, from_kdl_str};
    use saci_transformer::{Transformer, TransformerFactory};
    use saci_transformer_csv::CsvTransformerFactory;
    use saci_transformer_ndjson::NdjsonTransformerFactory;
    use saci_transformer_parquet::ParquetTransformerFactory;
    use tempfile::TempDir;

    use super::*;

    fn csv_transformer(options: &ConfigValue) -> Arc<dyn Transformer> {
        CsvTransformerFactory
            .build(options)
            .expect("csv transformer builds")
    }

    fn parquet_transformer() -> Arc<dyn Transformer> {
        ParquetTransformerFactory
            .build(&ConfigValue::Object(ConfigMap::new()))
            .expect("parquet transformer builds")
    }

    fn ndjson_transformer() -> Arc<dyn Transformer> {
        NdjsonTransformerFactory
            .build(&ConfigValue::Object(ConfigMap::new()))
            .expect("ndjson transformer builds")
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
        assert_eq!(
            err.message(),
            "FileSource config requires a 'path' string field"
        );

        let Err(err) = FileSinkFactory.build(&empty_config(), &ctx) else {
            panic!("path is required");
        };
        assert_eq!(
            err.message(),
            "FileSink config requires a 'path' string field"
        );
    }

    #[test]
    fn a_source_with_no_bound_transformer_is_a_configuration_error() {
        let ctx = ConnectorContext::new(None);
        let dir = TempDir::new().expect("temp dir");
        let raw = format!("path \"{}\"\n{CSV_SCHEMA}", config_path(&dir, "in.csv"));

        let Err(err) = FileSourceFactory.build(&config(&raw), &ctx) else {
            panic!("a source that moves bytes needs a bound transformer");
        };
        assert_eq!(
            err.message(),
            "FileSource moves bytes and needs a 'transformer' key naming a declared transformer"
        );
    }

    #[test]
    fn parquet_with_declared_schema_fields_projects_onto_them() {
        let ctx = ConnectorContext::new(Some(parquet_transformer()));
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
        let payloads = parquet_transformer()
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
        let ctx = ConnectorContext::new(Some(csv_transformer(&empty_config())));
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
        let ctx = ConnectorContext::new(Some(csv_transformer(&ConfigValue::Object(options))));
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
        let ctx = ConnectorContext::new(Some(ndjson_transformer()));
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
