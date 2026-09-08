//! [`NdjsonTransformer`]: the `ndjson` format, both surfaces.

use std::io::{BufReader, BufWriter, Write};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_json::ReaderBuilder;
use arrow_json::writer::LineDelimitedWriter;
use arrow_schema::Schema;

use saci_core::error::SaciError;
use saci_transformer::{
    BatchReader, BatchWriter, ConfigValue, MessageDecoder, MessageShape, Transformer,
    TransformerFactory,
};

/// Records inspected when inferring a schema, unless `infer_max` says
/// otherwise.
const DEFAULT_INFER_MAX: usize = 1024;

/// The `ndjson` byte format: one JSON object per line.
pub struct NdjsonTransformer {
    infer_max: usize,
}

impl NdjsonTransformer {
    /// Build the format, inspecting at most `infer_max` records when it has to
    /// infer a schema.
    pub fn new(infer_max: usize) -> Self {
        Self { infer_max }
    }
}

impl Default for NdjsonTransformer {
    fn default() -> Self {
        Self::new(DEFAULT_INFER_MAX)
    }
}

impl Transformer for NdjsonTransformer {
    fn format(&self) -> &'static str {
        "ndjson"
    }

    fn open_reader(
        &self,
        input: std::fs::File,
        declared: Option<Arc<Schema>>,
    ) -> Result<Box<dyn BatchReader>, SaciError> {
        let mut buffered = BufReader::new(input);
        let schema = match declared {
            Some(schema) => schema,
            // Seeks back to the start itself, so the reader below still sees
            // the whole file.
            None => {
                let (inferred, _records) = arrow_json::reader::infer_json_schema_from_seekable(
                    &mut buffered,
                    Some(self.infer_max),
                )
                .map_err(|e| SaciError::generic(format!("ndjson: schema inference failed: {e}")))?;
                Arc::new(inferred)
            }
        };

        let reader = ReaderBuilder::new(Arc::clone(&schema))
            .build(buffered)
            .map_err(|e| SaciError::generic(format!("ndjson: reader build failed: {e}")))?;
        Ok(Box::new(NdjsonBatchReader { reader, schema }))
    }

    fn open_writer(
        &self,
        output: Box<dyn Write + Send>,
        schema: Arc<Schema>,
    ) -> Result<Box<dyn BatchWriter>, SaciError> {
        let _ = schema;
        Ok(Box::new(NdjsonBatchWriter {
            writer: LineDelimitedWriter::new(BufWriter::new(output)),
        }))
    }

    fn open_message_decoder(
        &self,
        schema: Arc<Schema>,
    ) -> Result<Box<dyn MessageDecoder>, SaciError> {
        let decoder = ReaderBuilder::new(schema)
            .build_decoder()
            .map_err(|e| SaciError::generic(format!("ndjson: decoder build failed: {e}")))?;
        Ok(Box::new(NdjsonMessageDecoder { decoder }))
    }

    fn encode_messages(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>, SaciError> {
        let mut writer = LineDelimitedWriter::new(Vec::new());
        writer
            .write(batch)
            .map_err(|e| SaciError::generic(format!("ndjson: encode error: {e}")))?;
        writer
            .finish()
            .map_err(|e| SaciError::generic(format!("ndjson: encode error: {e}")))?;
        Ok(writer
            .into_inner()
            .split(|&b| b == b'\n')
            .filter(|line| !line.is_empty())
            .map(<[u8]>::to_vec)
            .collect())
    }

    fn message_shape(&self) -> Option<MessageShape> {
        Some(MessageShape::PerRow)
    }
}

/// Factory for [`NdjsonTransformer`].
///
/// `options`:
/// - `infer_max` (integer, optional, default `1024`): records inspected when
///   no schema is declared, written `options infer_max=4096`.
pub struct NdjsonTransformerFactory;

impl TransformerFactory for NdjsonTransformerFactory {
    fn format_name(&self) -> &'static str {
        "ndjson"
    }

    fn build(&self, options: &ConfigValue) -> Result<Arc<dyn Transformer>, SaciError> {
        let infer_max = match options.get("infer_max") {
            None => DEFAULT_INFER_MAX,
            Some(value) => {
                let raw = value.as_i64().ok_or_else(|| {
                    SaciError::configuration("ndjson: option 'infer_max' must be an integer")
                })?;
                if raw < 1 {
                    return Err(SaciError::configuration(
                        "ndjson: option 'infer_max' must be at least 1",
                    ));
                }
                raw as usize
            }
        };
        Ok(Arc::new(NdjsonTransformer::new(infer_max)))
    }
}

struct NdjsonBatchReader {
    reader: arrow_json::Reader<BufReader<std::fs::File>>,
    schema: Arc<Schema>,
}

impl BatchReader for NdjsonBatchReader {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        match self.reader.next() {
            None => Ok(None),
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => Err(SaciError::generic(format!("ndjson: read error: {e}"))),
        }
    }
}

struct NdjsonBatchWriter {
    writer: LineDelimitedWriter<BufWriter<Box<dyn Write + Send>>>,
}

impl BatchWriter for NdjsonBatchWriter {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        self.writer
            .write(batch)
            .map_err(|e| SaciError::generic(format!("ndjson: write error: {e}")))
    }

    fn finish(mut self: Box<Self>) -> Result<(), SaciError> {
        self.writer
            .finish()
            .map_err(|e| SaciError::generic(format!("ndjson: finish error: {e}")))?;
        let mut buffered = self.writer.into_inner();
        buffered
            .flush()
            .map_err(|e| SaciError::generic(format!("ndjson: finish error: {e}")))
    }
}

struct NdjsonMessageDecoder {
    decoder: arrow_json::reader::Decoder,
}

impl MessageDecoder for NdjsonMessageDecoder {
    fn push(&mut self, payload: &[u8]) -> Result<(), SaciError> {
        // The decoder consumes whole records, so each payload is followed by the
        // newline that terminates its object. `decode` returns the bytes it
        // took, which is why this loops rather than asserting one call is
        // enough.
        for chunk in [payload, b"\n".as_slice()] {
            let mut remaining = chunk;
            while !remaining.is_empty() {
                let read = self
                    .decoder
                    .decode(remaining)
                    .map_err(|e| SaciError::generic(format!("ndjson: decode: {e}")))?;
                if read == 0 {
                    break;
                }
                remaining = &remaining[read..];
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        self.decoder
            .flush()
            .map_err(|e| SaciError::generic(format!("ndjson: flush: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int32Array, Int64Array};
    use arrow_schema::{DataType, Field};
    use tempfile::NamedTempFile;

    use super::*;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Float64, false),
        ]))
    }

    fn write_ndjson(lines: &[&str]) -> NamedTempFile {
        let mut f = NamedTempFile::new().expect("temp file");
        for line in lines {
            writeln!(f, "{line}").expect("line");
        }
        f.flush().expect("flush");
        f
    }

    fn read_all(
        transformer: &NdjsonTransformer,
        file: std::fs::File,
        declared: Option<Arc<Schema>>,
    ) -> (Arc<Schema>, Vec<RecordBatch>) {
        let mut reader = transformer
            .open_reader(file, declared)
            .expect("reader opens");
        let schema = reader.schema();
        let mut batches = Vec::new();
        while let Some(batch) = reader.next_batch().expect("read") {
            batches.push(batch);
        }
        (schema, batches)
    }

    #[test]
    fn a_declared_schema_governs_the_read() {
        let f = write_ndjson(&[
            r#"{"id":1,"val":1.5}"#,
            r#"{"id":2,"val":2.5}"#,
            r#"{"id":3,"val":3.5}"#,
        ]);
        let (read_schema, batches) = read_all(
            &NdjsonTransformer::default(),
            f.reopen().expect("reopen"),
            Some(schema()),
        );
        assert_eq!(read_schema.fields().len(), 2);
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    }

    #[test]
    fn an_absent_schema_is_inferred_from_the_first_records() {
        let f = write_ndjson(&[r#"{"x":10}"#, r#"{"x":20}"#]);
        let (read_schema, batches) = read_all(
            &NdjsonTransformer::default(),
            f.reopen().expect("reopen"),
            None,
        );
        assert_eq!(read_schema.fields().len(), 1);
        assert_eq!(read_schema.field(0).name(), "x");
        // Inference rewinds, so no record is consumed by it.
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    }

    #[test]
    fn a_write_then_read_round_trip_preserves_every_value() {
        let one_column = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&one_column),
            vec![Arc::new(Int32Array::from_iter_values(0i32..5))],
        )
        .expect("batch");

        let transformer = NdjsonTransformer::default();
        let file = NamedTempFile::new().expect("temp file");
        let writer = transformer
            .open_writer(
                Box::new(file.reopen().expect("reopen for write")),
                Arc::clone(&one_column),
            )
            .expect("writer opens");
        let mut writer = writer;
        writer.write_batch(&batch).expect("write");
        writer.finish().expect("finish");

        let (_, batches) = read_all(
            &transformer,
            file.reopen().expect("reopen for read"),
            Some(Arc::clone(&one_column)),
        );
        let read = &batches[0];
        let column = read
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int column");
        assert_eq!(read.num_rows(), 5);
        for i in 0i32..5 {
            assert_eq!(column.value(i as usize), i);
        }
    }

    #[test]
    fn a_window_of_messages_decodes_into_one_batch() {
        let one_column = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let transformer = NdjsonTransformer::default();
        let mut decoder = transformer
            .open_message_decoder(Arc::clone(&one_column))
            .expect("decoder opens");

        decoder.push(br#"{"a":42}"#).expect("first message");
        decoder.push(br#"{"a":99}"#).expect("second message");
        let batch = decoder.flush().expect("flush").expect("two rows");

        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int column");
        assert_eq!(column.value(0), 42);
        assert_eq!(column.value(1), 99);

        // `flush` resets, so the decoder serves the next window too.
        assert!(decoder.flush().expect("second flush").is_none());
    }

    #[test]
    fn a_malformed_payload_is_reported_by_push() {
        let one_column = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let mut decoder = NdjsonTransformer::default()
            .open_message_decoder(one_column)
            .expect("decoder opens");
        // A syntax error surfaces where it is fed. A merely truncated object
        // does not: the decoder is a streaming one, and an unfinished record is
        // indistinguishable from a record whose rest has not arrived yet.
        let Err(err) = decoder.push(b"}{") else {
            panic!("a payload that is not JSON must be rejected");
        };
        assert!(err.message().contains("ndjson: decode"), "got: {err}");
    }

    #[test]
    fn encoding_emits_one_message_per_row() {
        let one_column = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            one_column,
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
        )
        .expect("batch");

        let transformer = NdjsonTransformer::default();
        assert_eq!(transformer.message_shape(), Some(MessageShape::PerRow));
        let payloads = transformer.encode_messages(&batch).expect("encode");
        assert_eq!(payloads.len(), 3);
        assert_eq!(payloads[0], br#"{"a":1}"#);
        assert!(
            payloads.iter().all(|p| !p.contains(&b'\n')),
            "a payload must not carry its line terminator"
        );
    }

    #[test]
    fn the_factory_reads_infer_max_off_its_options_table() {
        let options = saci_transformer::from_kdl_str("infer_max 4").expect("parse kdl");
        let transformer = NdjsonTransformerFactory.build(&options).expect("build");
        assert_eq!(transformer.format(), "ndjson");
        assert_eq!(NdjsonTransformerFactory.format_name(), "ndjson");

        // Only the first four records are inspected, so a fifth-record column
        // is not in the inferred schema.
        let f = write_ndjson(&[
            r#"{"a":1}"#,
            r#"{"a":2}"#,
            r#"{"a":3}"#,
            r#"{"a":4}"#,
            r#"{"a":5,"late":true}"#,
        ]);
        let reader = transformer
            .open_reader(f.reopen().expect("reopen"), None)
            .expect("reader opens");
        assert_eq!(reader.schema().fields().len(), 1);
    }

    #[test]
    fn a_non_integer_infer_max_is_a_configuration_error() {
        let options = saci_transformer::from_kdl_str("infer_max \"lots\"").expect("parse kdl");
        let Err(err) = NdjsonTransformerFactory.build(&options) else {
            panic!("infer_max must be an integer");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("infer_max"), "got: {err}");
    }

    #[test]
    fn a_zero_infer_max_is_a_configuration_error() {
        let options = saci_transformer::from_kdl_str("infer_max 0").expect("parse kdl");
        let Err(err) = NdjsonTransformerFactory.build(&options) else {
            panic!("infer_max must be at least 1");
        };
        assert!(err.message().contains("at least 1"), "got: {err}");
    }
}
