//! [`ParquetTransformer`]: the `parquet` format, both surfaces.

use std::io::Write;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_cast::CastOptions;
use arrow_schema::Schema;
use arrow_select::concat::concat_batches;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use saci_core::error::SaciError;
use saci_core::io::cast_batch;
use saci_transformer::{
    BatchReader, BatchWriter, ConfigValue, MessageDecoder, MessageShape, Transformer,
    TransformerFactory,
};

/// The `parquet` byte format: one self-contained Parquet file, whether that
/// file is a whole object or one message payload.
///
/// Snappy compression is fixed: it is the only setting a SACI pipeline has ever
/// written, and a checkpoint or dataset file that changes codec between runs is
/// a compatibility problem, not a tuning knob.
///
/// A Parquet file ends in a footer, so a payload has to be a whole file and the
/// message shape is [`MessageShape::PerBatch`]. That is the smallest unit
/// Parquet has, and it is not free: the magic bytes, the per-column page
/// headers and the Thrift footer come to about 470 bytes for a three-column
/// row type before a single row is written. That cost is paid once per batch,
/// so a message transport carrying this format wants batches, not rows.
#[derive(Default)]
pub struct ParquetTransformer;

impl ParquetTransformer {
    /// Build the format.
    pub fn new() -> Self {
        Self
    }
}

/// The one write configuration both surfaces use.
fn writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build()
}

impl Transformer for ParquetTransformer {
    fn format(&self) -> &'static str {
        "parquet"
    }

    fn open_reader(
        &self,
        input: std::fs::File,
        declared: Option<Arc<Schema>>,
    ) -> Result<Box<dyn BatchReader>, SaciError> {
        let builder = ParquetRecordBatchReaderBuilder::try_new(input)
            .map_err(|e| SaciError::generic(format!("parquet: builder error: {e}")))?;
        let file_schema = Arc::clone(builder.schema());

        // Row-group metadata is already in memory: no data pages are read.
        let rows: usize = builder
            .metadata()
            .row_groups()
            .iter()
            .map(|group| group.num_rows() as usize)
            .sum();
        let estimated_rows = (rows > 0).then_some(rows);

        // A declared schema is pushed into the reader as a column projection,
        // so the pages of a column it does not name are never read.
        let (builder, schema) = match &declared {
            None => (builder, file_schema),
            Some(declared) => {
                let indices = declared
                    .fields()
                    .iter()
                    .map(|field| {
                        file_schema.index_of(field.name()).map_err(|_| {
                            SaciError::configuration(format!(
                                "parquet: declared column '{}' is not in the file (file \
                                 columns: {})",
                                field.name(),
                                render_names(&file_schema),
                            ))
                        })
                    })
                    .collect::<Result<Vec<usize>, SaciError>>()?;
                // Root indices, not the `columns` name form: a field name
                // carrying a `.` is one name here and one column path there.
                let mask = ProjectionMask::roots(builder.parquet_schema(), indices);
                (builder.with_projection(mask), Arc::clone(declared))
            }
        };

        let reader = builder
            .build()
            .map_err(|e| SaciError::generic(format!("parquet: reader build error: {e}")))?;
        Ok(Box::new(ParquetBatchReader {
            reader,
            schema,
            declared,
            estimated_rows,
        }))
    }

    fn open_writer(
        &self,
        output: Box<dyn Write + Send>,
        schema: Arc<Schema>,
    ) -> Result<Box<dyn BatchWriter>, SaciError> {
        // `ArrowWriter` buffers a row group of its own, so the handle arrives
        // unwrapped and `close` flushes through it.
        let writer = ArrowWriter::try_new(output, schema, Some(writer_properties()))
            .map_err(|e| SaciError::generic(format!("parquet: writer init error: {e}")))?;
        Ok(Box::new(ParquetBatchWriter { writer }))
    }

    /// Open a decoder for discrete payloads, one whole Parquet file apiece.
    ///
    /// `schema` is the projection every payload is reduced to: a payload
    /// carrying more columns is read down to it, and one missing a column it
    /// names is refused.
    fn open_message_decoder(
        &self,
        schema: Arc<Schema>,
    ) -> Result<Box<dyn MessageDecoder>, SaciError> {
        Ok(Box::new(ParquetMessageDecoder {
            schema,
            batches: Vec::new(),
        }))
    }

    /// Encode the whole batch as one payload: a complete Parquet file, footer
    /// included.
    fn encode_messages(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>, SaciError> {
        let mut writer =
            ArrowWriter::try_new(Vec::new(), batch.schema(), Some(writer_properties()))
                .map_err(|e| SaciError::generic(format!("parquet: encode: {e}")))?;
        writer
            .write(batch)
            .map_err(|e| SaciError::generic(format!("parquet: encode: {e}")))?;
        // `into_inner` writes the footer, which is what makes the payload a
        // readable file rather than a prefix of one.
        let payload = writer
            .into_inner()
            .map_err(|e| SaciError::generic(format!("parquet: encode: {e}")))?;
        Ok(vec![payload])
    }

    fn message_shape(&self) -> Option<MessageShape> {
        Some(MessageShape::PerBatch)
    }
}

/// Factory for [`ParquetTransformer`]. Reads no options.
pub struct ParquetTransformerFactory;

impl TransformerFactory for ParquetTransformerFactory {
    fn format_name(&self) -> &'static str {
        "parquet"
    }

    fn build(&self, _options: &ConfigValue) -> Result<Arc<dyn Transformer>, SaciError> {
        Ok(Arc::new(ParquetTransformer::new()))
    }
}

/// The column names of `schema`, comma separated, for an error that has to name
/// what a file carries.
fn render_names(schema: &Schema) -> String {
    schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Project `batch` onto `schema`.
///
/// Columns the batch carries and `schema` does not are dropped; a column
/// `schema` requires and the batch lacks is an error. Identical fields skip the
/// rebuild. `safe: false`: a value that does not fit the declared type is an
/// error, never a silent null.
fn project(batch: RecordBatch, schema: &Schema) -> Result<RecordBatch, SaciError> {
    if batch.schema_ref().fields() == schema.fields() {
        return Ok(batch);
    }
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    cast_batch(&batch, schema, &options)
        .map_err(|e| SaciError::generic(format!("parquet: casting to the declared schema: {e}")))
}

struct ParquetBatchReader {
    reader: ParquetRecordBatchReader,
    /// What [`BatchReader::schema`] reports: `declared` when the config named
    /// one, the file's own otherwise.
    schema: Arc<Schema>,
    /// The projection, `None` when the file's own schema governs. The reader
    /// already dropped the columns, so this only narrows types and order.
    declared: Option<Arc<Schema>>,
    estimated_rows: Option<usize>,
}

impl BatchReader for ParquetBatchReader {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        match self.reader.next() {
            None => Ok(None),
            // A projection can only change a column's type or the field order,
            // never which rows come back.
            Some(Ok(batch)) => match &self.declared {
                Some(declared) => project(batch, declared).map(Some),
                None => Ok(Some(batch)),
            },
            Some(Err(e)) => Err(SaciError::generic(format!("parquet: read error: {e}"))),
        }
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.estimated_rows
    }
}

struct ParquetBatchWriter {
    writer: ArrowWriter<Box<dyn Write + Send>>,
}

impl BatchWriter for ParquetBatchWriter {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        self.writer
            .write(batch)
            .map_err(|e| SaciError::generic(format!("parquet: write error: {e}")))
    }

    fn finish(self: Box<Self>) -> Result<(), SaciError> {
        // Writes the footer and flushes the writer's own buffer down to the
        // handle. Without it the file has no readable metadata at all.
        self.writer
            .close()
            .map(|_metadata| ())
            .map_err(|e| SaciError::generic(format!("parquet: finish error: {e}")))
    }
}

/// One whole Parquet file per payload, folded into one batch per window.
struct ParquetMessageDecoder {
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
}

impl MessageDecoder for ParquetMessageDecoder {
    fn push(&mut self, payload: &[u8]) -> Result<(), SaciError> {
        // `ChunkReader` needs random access inside the payload, which the
        // footer at its end forces. A payload is already whole and in memory,
        // so `Bytes` gives that with one copy and no temporary file.
        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(payload))
            .map_err(|e| SaciError::generic(format!("parquet: file header: {e}")))?;

        // A payload carrying more columns than the declared schema projects
        // onto it, the way an object read does. A projection is pushed into
        // the reader so the extra columns' pages are never decoded; a declared
        // column the payload lacks is refused here rather than by the cast.
        let file_schema = builder.schema().clone();
        let indices = self
            .schema
            .fields()
            .iter()
            .map(|field| {
                file_schema.index_of(field.name()).map_err(|_| {
                    SaciError::generic(format!(
                        "parquet: payload's declared column '{}' is missing (payload \
                         columns: {})",
                        field.name(),
                        render_names(&file_schema),
                    ))
                })
            })
            .collect::<Result<Vec<usize>, SaciError>>()?;
        let mask = ProjectionMask::roots(builder.parquet_schema(), indices);

        let reader = builder
            .with_projection(mask)
            .build()
            .map_err(|e| SaciError::generic(format!("parquet: reader build error: {e}")))?;
        for batch in reader {
            let batch = batch.map_err(|e| SaciError::generic(format!("parquet: decode: {e}")))?;
            self.batches.push(project(batch, &self.schema)?);
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        if self.batches.is_empty() {
            return Ok(None);
        }
        let batch = concat_batches(&self.schema, self.batches.iter())
            .map_err(|e| SaciError::generic(format!("parquet: concatenating batches: {e}")))?;
        self.batches.clear();
        Ok(Some(batch))
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Float64Array, Int64Array, UInt64Array};
    use arrow_schema::{DataType, Field};
    use tempfile::NamedTempFile;

    use saci_transformer::ConfigMap;

    use super::*;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Float64, false),
        ]))
    }

    fn batch(rows: i64) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from_iter_values(0..rows)),
                Arc::new(Float64Array::from_iter_values(
                    (0..rows).map(|i| i as f64 * 1.5),
                )),
            ],
        )
        .expect("batch")
    }

    /// Write `rows` rows through the transformer and hand back the file.
    fn written(rows: i64) -> NamedTempFile {
        written_with(schema(), &batch(rows))
    }

    /// The same, for a file whose own schema is not the two-column default.
    fn written_with(schema: Arc<Schema>, batch: &RecordBatch) -> NamedTempFile {
        let file = NamedTempFile::new().expect("temp file");
        let mut writer = ParquetTransformer::new()
            .open_writer(Box::new(file.reopen().expect("reopen for write")), schema)
            .expect("writer opens");
        writer.write_batch(batch).expect("write");
        writer.finish().expect("close writes the footer");
        file
    }

    #[test]
    fn the_schema_comes_from_the_file() {
        let file = written(10);
        let reader = ParquetTransformer::new()
            .open_reader(file.reopen().expect("reopen"), None)
            .expect("reader opens");
        let schema = reader.schema();
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "val");
    }

    /// The file's own three columns: one more than `schema` projects onto.
    fn wide_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Float64, false),
            Field::new("extra", DataType::Int64, false),
        ]))
    }

    fn wide_batch(rows: i64) -> RecordBatch {
        RecordBatch::try_new(
            wide_schema(),
            vec![
                Arc::new(Int64Array::from_iter_values(0..rows)),
                Arc::new(Float64Array::from_iter_values(
                    (0..rows).map(|i| i as f64 * 1.5),
                )),
                Arc::new(Int64Array::from_iter_values(0..rows)),
            ],
        )
        .expect("batch")
    }

    #[test]
    fn a_declared_schema_projects_a_wider_file() {
        let file = written_with(wide_schema(), &wide_batch(5));

        let mut reader = ParquetTransformer::new()
            .open_reader(file.reopen().expect("reopen"), Some(schema()))
            .expect("a declared schema is a projection target");
        assert_eq!(reader.schema(), schema(), "the declared schema governs");
        assert_eq!(
            reader.estimated_rows(),
            Some(5),
            "the row count stays the file's"
        );

        let projected = reader.next_batch().expect("read").expect("one batch");
        assert_eq!(projected.num_columns(), 2);
        assert_eq!(projected.schema_ref().fields(), schema().fields());
        assert!(
            projected.schema().field_with_name("extra").is_err(),
            "the unprojected column is gone"
        );
        assert_eq!(projected, batch(5), "the surviving columns are untouched");
    }

    #[test]
    fn a_declared_column_the_file_lacks_is_a_configuration_error_naming_it() {
        let declared = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("missing", DataType::Int64, false),
        ]));
        let file = written(1);

        let Err(err) =
            ParquetTransformer::new().open_reader(file.reopen().expect("reopen"), Some(declared))
        else {
            panic!("the file carries no 'missing' column");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("missing"), "got: {err}");
        assert!(
            err.message().contains("id, val"),
            "the error names the file's own columns: {err}"
        );
    }

    #[test]
    fn estimated_rows_comes_from_row_group_metadata() {
        let file = written(42);
        let reader = ParquetTransformer::new()
            .open_reader(file.reopen().expect("reopen"), None)
            .expect("reader opens");
        assert_eq!(reader.estimated_rows(), Some(42));
    }

    #[test]
    fn a_write_then_read_round_trip_preserves_every_row() {
        let file = written(1_000);
        let mut reader = ParquetTransformer::new()
            .open_reader(file.reopen().expect("reopen"), None)
            .expect("reader opens");
        let mut rows = 0;
        while let Some(batch) = reader.next_batch().expect("read") {
            rows += batch.num_rows();
        }
        assert_eq!(rows, 1_000);
    }

    #[test]
    fn the_factory_builds_the_format_and_reads_no_options() {
        let transformer = ParquetTransformerFactory
            .build(&ConfigValue::Object(ConfigMap::new()))
            .expect("build");
        assert_eq!(transformer.format(), "parquet");
        assert_eq!(ParquetTransformerFactory.format_name(), "parquet");
    }

    #[test]
    fn the_message_shape_is_one_payload_per_batch() {
        assert_eq!(
            ParquetTransformer::new().message_shape(),
            Some(MessageShape::PerBatch)
        );
    }

    #[test]
    fn encoding_emits_one_self_contained_file_for_the_whole_batch() {
        let payloads = ParquetTransformer::new()
            .encode_messages(&batch(3))
            .expect("encode");
        assert_eq!(payloads.len(), 1);
        let payload = &payloads[0];
        assert_eq!(
            &payload[..4],
            b"PAR1",
            "a Parquet file opens with its magic"
        );
        assert_eq!(
            &payload[payload.len() - 4..],
            b"PAR1",
            "and closes with it, after the footer"
        );
    }

    #[test]
    fn a_message_round_trip_preserves_every_row_and_column_type() {
        let transformer = ParquetTransformer::new();
        let original = batch(500);
        let payloads = transformer.encode_messages(&original).expect("encode");
        let mut decoder = transformer
            .open_message_decoder(schema())
            .expect("decoder opens");
        decoder.push(&payloads[0]).expect("push");
        let decoded = decoder.flush().expect("flush").expect("one batch");
        assert_eq!(decoded, original);
    }

    #[test]
    fn every_payload_of_a_window_folds_into_one_batch() {
        let transformer = ParquetTransformer::new();
        let mut decoder = transformer
            .open_message_decoder(schema())
            .expect("decoder opens");
        for _ in 0..3 {
            let payloads = transformer.encode_messages(&batch(4)).expect("encode");
            decoder.push(&payloads[0]).expect("push");
        }
        let decoded = decoder.flush().expect("flush").expect("one batch");
        assert_eq!(decoded.num_rows(), 12);
        // The window is consumed, so a second flush has nothing to hand back.
        assert!(decoder.flush().expect("flush").is_none());
    }

    #[test]
    fn an_unsigned_column_survives_the_message_round_trip_unwidened() {
        let declared = Arc::new(Schema::new(vec![Field::new(
            "seq",
            DataType::UInt64,
            false,
        )]));
        let original = RecordBatch::try_new(
            Arc::clone(&declared),
            vec![Arc::new(UInt64Array::from(vec![1u64, 2, u64::MAX]))],
        )
        .expect("batch");

        let transformer = ParquetTransformer::new();
        let payloads = transformer.encode_messages(&original).expect("encode");
        let mut decoder = transformer
            .open_message_decoder(declared)
            .expect("decoder opens");
        decoder.push(&payloads[0]).expect("push");
        let decoded = decoder.flush().expect("flush").expect("one batch");
        assert_eq!(decoded.schema_ref().field(0).data_type(), &DataType::UInt64);
        assert_eq!(decoded, original);
    }

    #[test]
    fn a_payload_carrying_extra_columns_is_projected() {
        let payloads = ParquetTransformer::new()
            .encode_messages(&wide_batch(4))
            .expect("encode");
        let mut decoder = ParquetTransformer::new()
            .open_message_decoder(schema())
            .expect("decoder opens");
        decoder
            .push(&payloads[0])
            .expect("a superset payload projects");
        let decoded = decoder.flush().expect("flush").expect("one batch");
        assert_eq!(decoded.schema(), schema());
        assert_eq!(decoded, batch(4), "the surviving columns are untouched");
    }

    #[test]
    fn a_payload_missing_a_declared_column_is_refused() {
        let other = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let payloads = ParquetTransformer::new()
            .encode_messages(
                &RecordBatch::try_new(other, vec![Arc::new(Int64Array::from(vec![1i64]))])
                    .expect("batch"),
            )
            .expect("encode");
        let mut decoder = ParquetTransformer::new()
            .open_message_decoder(schema())
            .expect("decoder opens");
        let Err(err) = decoder.push(&payloads[0]) else {
            panic!("the payload carries no 'val' column");
        };
        assert!(err.message().contains("val"), "got: {err}");
        assert!(
            err.message().contains("id"),
            "the error names the payload's own columns: {err}"
        );
    }

    #[test]
    fn a_payload_that_is_not_a_parquet_file_is_refused() {
        let mut decoder = ParquetTransformer::new()
            .open_message_decoder(schema())
            .expect("decoder opens");
        let Err(err) = decoder.push(b"id,val\n1,1.5\n") else {
            panic!("only a whole Parquet file decodes");
        };
        assert!(err.message().contains("file header"), "got: {err}");
    }
}
