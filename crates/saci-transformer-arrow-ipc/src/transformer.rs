//! [`ArrowIpcTransformer`]: the `arrow-ipc` format, both surfaces.

use std::io::{BufReader, BufWriter, Write};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_cast::CastOptions;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::Schema;
use arrow_select::concat::concat_batches;

use saci_core::error::SaciError;
use saci_core::io::cast_batch;
use saci_transformer::{
    BatchReader, BatchWriter, ConfigValue, MessageDecoder, MessageShape, Transformer,
    TransformerFactory,
};

/// The `arrow-ipc` byte format: one Arrow IPC stream, whether that stream is a
/// whole file or one message payload.
///
/// Both surfaces carry the same encapsulation, the Arrow **stream** format
/// (`StreamWriter`/`StreamReader`): a schema message, then one message per
/// `RecordBatch`, then an end-of-stream marker. The random-access file format
/// would buy nothing here. [`BatchReader`] pulls forward only, and every
/// consumer of the read surface is a local file or an object already being
/// spooled, so a footer index is never consulted. It would also leave the two
/// surfaces mutually unreadable.
#[derive(Default)]
pub struct ArrowIpcTransformer;

impl ArrowIpcTransformer {
    /// Build the format.
    pub fn new() -> Self {
        Self
    }
}

impl Transformer for ArrowIpcTransformer {
    fn format(&self) -> &'static str {
        "arrow-ipc"
    }

    fn open_reader(
        &self,
        input: std::fs::File,
        declared: Option<Arc<Schema>>,
    ) -> Result<Box<dyn BatchReader>, SaciError> {
        // The schema message is read here, so a handle whose bytes are not an
        // IPC stream is refused at open rather than part way through, and the
        // same `stream header` text the message surface reports names it.
        let reader = StreamReader::try_new(BufReader::new(input), None)
            .map_err(|e| SaciError::generic(format!("arrow-ipc: stream header: {e}")))?;
        // The stream's own schema unless the config projected onto a declared
        // one: an IPC stream stores no column subset, so a projection is
        // applied to every decoded batch rather than to the read.
        let schema = match &declared {
            Some(declared) => Arc::clone(declared),
            None => reader.schema(),
        };
        Ok(Box::new(ArrowIpcBatchReader {
            reader,
            schema,
            declared,
        }))
    }

    fn open_writer(
        &self,
        output: Box<dyn Write + Send>,
        schema: Arc<Schema>,
    ) -> Result<Box<dyn BatchWriter>, SaciError> {
        // The schema message goes out here, so a run that writes no batch
        // still leaves a valid, readable, zero-row stream.
        let writer = StreamWriter::try_new(BufWriter::new(output), schema.as_ref())
            .map_err(|e| SaciError::generic(format!("arrow-ipc: writer init error: {e}")))?;
        Ok(Box::new(ArrowIpcBatchWriter { writer }))
    }

    fn open_message_decoder(
        &self,
        schema: Arc<Schema>,
    ) -> Result<Box<dyn MessageDecoder>, SaciError> {
        Ok(Box::new(ArrowIpcMessageDecoder {
            schema,
            batches: Vec::new(),
        }))
    }

    fn encode_messages(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>, SaciError> {
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, batch.schema_ref())
                .map_err(|e| SaciError::generic(format!("arrow-ipc: encode: {e}")))?;
            writer
                .write(batch)
                .map_err(|e| SaciError::generic(format!("arrow-ipc: encode: {e}")))?;
            writer
                .finish()
                .map_err(|e| SaciError::generic(format!("arrow-ipc: encode: {e}")))?;
        }
        Ok(vec![buf])
    }

    fn message_shape(&self) -> Option<MessageShape> {
        Some(MessageShape::PerBatch)
    }
}

/// Factory for [`ArrowIpcTransformer`]. Reads no options.
pub struct ArrowIpcTransformerFactory;

impl TransformerFactory for ArrowIpcTransformerFactory {
    fn format_name(&self) -> &'static str {
        "arrow-ipc"
    }

    fn build(&self, _options: &ConfigValue) -> Result<Arc<dyn Transformer>, SaciError> {
        Ok(Arc::new(ArrowIpcTransformer::new()))
    }
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
        .map_err(|e| SaciError::generic(format!("arrow-ipc: casting to the declared schema: {e}")))
}

struct ArrowIpcBatchReader {
    reader: StreamReader<BufReader<std::fs::File>>,
    /// What [`BatchReader::schema`] reports: `declared` when the config named
    /// one, the stream's own otherwise.
    schema: Arc<Schema>,
    /// The projection, `None` when the stream's own schema governs.
    declared: Option<Arc<Schema>>,
}

impl BatchReader for ArrowIpcBatchReader {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        match self.reader.next() {
            None => Ok(None),
            // The stream's own field types need not be the declared ones, so a
            // projection casts as well as drops.
            Some(Ok(batch)) => match &self.declared {
                Some(declared) => project(batch, declared).map(Some),
                None => Ok(Some(batch)),
            },
            Some(Err(e)) => Err(SaciError::generic(format!("arrow-ipc: read error: {e}"))),
        }
    }
}

struct ArrowIpcBatchWriter {
    writer: StreamWriter<BufWriter<Box<dyn Write + Send>>>,
}

impl BatchWriter for ArrowIpcBatchWriter {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        self.writer
            .write(batch)
            .map_err(|e| SaciError::generic(format!("arrow-ipc: write error: {e}")))
    }

    fn finish(mut self: Box<Self>) -> Result<(), SaciError> {
        // Writes the end-of-stream marker, then flushes the `BufWriter` down
        // to the handle. The flush is what makes this load-bearing: a
        // `StreamReader` treats plain EOF as a clean end of stream, so a
        // skipped `finish` loses the buffered tail silently, up to and
        // including the schema message of a stream that wrote one batch.
        self.writer
            .finish()
            .map_err(|e| SaciError::generic(format!("arrow-ipc: finish error: {e}")))
    }
}

struct ArrowIpcMessageDecoder {
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
}

impl MessageDecoder for ArrowIpcMessageDecoder {
    fn push(&mut self, payload: &[u8]) -> Result<(), SaciError> {
        let reader = StreamReader::try_new(std::io::Cursor::new(payload), None)
            .map_err(|e| SaciError::generic(format!("arrow-ipc: stream header: {e}")))?;
        for batch in reader {
            let batch = batch.map_err(|e| SaciError::generic(format!("arrow-ipc: decode: {e}")))?;
            // A payload carrying a superset of the declared schema projects
            // onto it rather than being refused.
            self.batches.push(project(batch, &self.schema)?);
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        if self.batches.is_empty() {
            return Ok(None);
        }
        let batch = concat_batches(&self.schema, self.batches.iter())
            .map_err(|e| SaciError::generic(format!("arrow-ipc: concatenating batches: {e}")))?;
        self.batches.clear();
        Ok(Some(batch))
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int32Array, Int64Array};
    use arrow_schema::{DataType, Field};
    use tempfile::NamedTempFile;

    use saci_transformer::ConfigMap;

    use super::*;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    fn batch(values: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(values))]).expect("batch")
    }

    fn encode(batch: &RecordBatch) -> Vec<u8> {
        let mut payloads = ArrowIpcTransformer::new()
            .encode_messages(batch)
            .expect("encode");
        assert_eq!(payloads.len(), 1, "arrow-ipc emits one message per batch");
        payloads.remove(0)
    }

    #[test]
    fn a_payload_round_trips_through_the_decoder() {
        let payload = encode(&batch(vec![7, 8]));
        let mut decoder = ArrowIpcTransformer::new()
            .open_message_decoder(schema())
            .expect("decoder opens");
        decoder.push(&payload).expect("push");
        let decoded = decoder.flush().expect("flush").expect("one batch");
        assert_eq!(decoded.num_rows(), 2);
    }

    #[test]
    fn a_window_of_payloads_is_concatenated() {
        let first = encode(&batch(vec![1, 2]));
        let second = encode(&batch(vec![3]));
        let mut decoder = ArrowIpcTransformer::new()
            .open_message_decoder(schema())
            .expect("decoder opens");
        decoder.push(&first).expect("first");
        decoder.push(&second).expect("second");
        let decoded = decoder.flush().expect("flush").expect("one batch");
        assert_eq!(decoded.num_rows(), 3);
        // `flush` reset the accumulator, so an empty window is `None`.
        assert!(decoder.flush().expect("second flush").is_none());
    }

    #[test]
    fn the_decoder_projects_a_payload_carrying_extra_columns() {
        let payload = encode(&wide_batch(vec![1, 2]));

        let mut decoder = ArrowIpcTransformer::new()
            .open_message_decoder(schema())
            .expect("decoder opens");
        decoder.push(&payload).expect("a superset payload projects");
        let decoded = decoder.flush().expect("flush").expect("one batch");
        assert_eq!(decoded.schema(), schema());
        assert_eq!(values(&[decoded]), vec![1, 2]);
    }

    #[test]
    fn the_decoder_casts_a_payload_whose_column_type_differs() {
        let narrow = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let sent = RecordBatch::try_new(
            Arc::clone(&narrow),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )
        .expect("batch");
        let payload = encode(&sent);

        let mut decoder = ArrowIpcTransformer::new()
            .open_message_decoder(schema())
            .expect("decoder opens");
        decoder
            .push(&payload)
            .expect("Int32 casts to the declared Int64");
        let decoded = decoder.flush().expect("flush").expect("one batch");
        assert_eq!(decoded.schema(), schema());
        assert_eq!(values(&[decoded]), vec![1, 2]);
    }

    #[test]
    fn the_decoder_refuses_a_payload_missing_a_declared_column() {
        let payload = encode(&batch(vec![1]));

        let mut decoder = ArrowIpcTransformer::new()
            .open_message_decoder(wide_schema())
            .expect("decoder opens");
        let Err(err) = decoder.push(&payload) else {
            panic!("the payload carries no 'w' column");
        };
        assert!(err.message().contains('w'), "got: {err}");
    }

    #[test]
    fn a_payload_that_is_not_an_ipc_stream_is_rejected_by_its_header() {
        let mut decoder = ArrowIpcTransformer::new()
            .open_message_decoder(schema())
            .expect("decoder opens");
        let Err(err) = decoder.push(b"not an arrow stream") else {
            panic!("a non-IPC payload must be rejected");
        };
        assert!(err.message().contains("stream header"), "got: {err}");
    }

    #[test]
    fn the_factory_builds_the_format_and_reads_no_options() {
        let transformer = ArrowIpcTransformerFactory
            .build(&ConfigValue::Object(ConfigMap::new()))
            .expect("build");
        assert_eq!(transformer.format(), "arrow-ipc");
        assert_eq!(transformer.message_shape(), Some(MessageShape::PerBatch));
        assert_eq!(ArrowIpcTransformerFactory.format_name(), "arrow-ipc");
    }

    fn write_stream(transformer: &dyn Transformer, batches: &[RecordBatch]) -> NamedTempFile {
        write_stream_with(transformer, schema(), batches)
    }

    /// The same, for a stream whose own schema is not the one-column default.
    fn write_stream_with(
        transformer: &dyn Transformer,
        schema: Arc<Schema>,
        batches: &[RecordBatch],
    ) -> NamedTempFile {
        let file = NamedTempFile::new().expect("temp file");
        let mut writer = transformer
            .open_writer(Box::new(file.reopen().expect("reopen for write")), schema)
            .expect("writer opens");
        for batch in batches {
            writer.write_batch(batch).expect("write");
        }
        writer
            .finish()
            .expect("finish writes the end-of-stream marker");
        file
    }

    fn read_all(transformer: &dyn Transformer, file: &NamedTempFile) -> Vec<RecordBatch> {
        let mut reader = transformer
            .open_reader(file.reopen().expect("reopen"), None)
            .expect("reader opens");
        let mut batches = Vec::new();
        while let Some(batch) = reader.next_batch().expect("read") {
            batches.push(batch);
        }
        batches
    }

    /// Every `v` in the order it was read.
    fn values(batches: &[RecordBatch]) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("v is Int64")
                    .values()
                    .to_vec()
            })
            .collect()
    }

    #[test]
    fn a_write_then_read_round_trip_preserves_every_row() {
        let transformer = ArrowIpcTransformer::new();
        let file = write_stream(&transformer, &[batch(vec![1, 2]), batch(vec![3])]);
        assert_eq!(values(&read_all(&transformer, &file)), vec![1, 2, 3]);
    }

    #[test]
    fn one_written_batch_is_one_message_on_the_way_back() {
        let transformer = ArrowIpcTransformer::new();
        let file = write_stream(&transformer, &[batch(vec![1, 2]), batch(vec![3])]);
        let shapes: Vec<usize> = read_all(&transformer, &file)
            .iter()
            .map(RecordBatch::num_rows)
            .collect();
        assert_eq!(shapes, vec![2, 1]);
    }

    #[test]
    fn the_schema_comes_from_the_stream() {
        let transformer = ArrowIpcTransformer::new();
        let file = write_stream(&transformer, &[batch(vec![4])]);
        let reader = transformer
            .open_reader(file.reopen().expect("reopen"), None)
            .expect("reader opens");
        assert_eq!(reader.schema().fields(), schema().fields());
    }

    /// The stream's own two columns: one more than `schema` projects onto.
    fn wide_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, false),
            Field::new("w", DataType::Int32, false),
        ]))
    }

    fn wide_batch(values: Vec<i64>) -> RecordBatch {
        let extra = Int32Array::from_iter_values(0..values.len() as i32);
        RecordBatch::try_new(
            wide_schema(),
            vec![Arc::new(Int64Array::from(values)), Arc::new(extra)],
        )
        .expect("batch")
    }

    #[test]
    fn a_declared_schema_projects_a_wider_stream() {
        let file = write_stream_with(
            &ArrowIpcTransformer::new(),
            wide_schema(),
            &[wide_batch(vec![1, 2])],
        );
        let mut reader = ArrowIpcTransformer::new()
            .open_reader(file.reopen().expect("reopen"), Some(schema()))
            .expect("a declared schema is a projection target");

        assert_eq!(reader.schema(), schema(), "the declared schema governs");
        let batch = reader.next_batch().expect("read").expect("one batch");
        assert_eq!(batch.num_columns(), 1);
        assert!(
            batch.schema().field_with_name("w").is_err(),
            "the unprojected column is gone"
        );
        assert_eq!(values(std::slice::from_ref(&batch)), vec![1, 2]);
    }

    #[test]
    fn a_declared_column_the_stream_lacks_is_an_error_naming_it() {
        let declared = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, false),
            Field::new("missing", DataType::Int64, false),
        ]));
        let file = write_stream(&ArrowIpcTransformer::new(), &[batch(vec![1])]);

        let mut reader = ArrowIpcTransformer::new()
            .open_reader(file.reopen().expect("reopen"), Some(declared))
            .expect("the schema message reads; the projection applies per batch");
        let Err(err) = reader.next_batch() else {
            panic!("the stream carries no 'missing' column");
        };
        assert!(err.message().contains("missing"), "got: {err}");
    }

    #[test]
    fn a_run_that_writes_no_batch_still_leaves_a_readable_stream() {
        let transformer = ArrowIpcTransformer::new();
        let file = write_stream(&transformer, &[]);
        let mut reader = transformer
            .open_reader(file.reopen().expect("reopen"), None)
            .expect("the schema message alone is a readable stream");
        assert!(reader.next_batch().expect("read").is_none());
    }

    #[test]
    fn a_handle_that_is_not_an_ipc_stream_is_rejected_by_its_header() {
        let mut file = NamedTempFile::new().expect("temp file");
        file.write_all(b"not an arrow stream").expect("write");
        file.flush().expect("flush");
        let Err(err) = ArrowIpcTransformer::new().open_reader(file.reopen().expect("reopen"), None)
        else {
            panic!("a non-IPC handle must be rejected at open");
        };
        assert!(err.message().contains("stream header"), "got: {err}");
    }

    #[test]
    fn a_written_stream_decodes_through_the_message_surface() {
        let transformer = ArrowIpcTransformer::new();
        let file = write_stream(&transformer, &[batch(vec![5, 6])]);
        let bytes = std::fs::read(file.path()).expect("read back the written stream");

        let mut decoder = transformer
            .open_message_decoder(schema())
            .expect("decoder opens");
        decoder.push(&bytes).expect("push the whole stream");
        let decoded = decoder.flush().expect("flush").expect("one batch");
        assert_eq!(values(&[decoded]), vec![5, 6]);
    }

    #[test]
    fn a_message_payload_opens_through_the_read_surface() {
        let payload = encode(&batch(vec![9]));
        let mut file = NamedTempFile::new().expect("temp file");
        file.write_all(&payload).expect("write");
        file.flush().expect("flush");

        let transformer = ArrowIpcTransformer::new();
        assert_eq!(values(&read_all(&transformer, &file)), vec![9]);
    }
}
