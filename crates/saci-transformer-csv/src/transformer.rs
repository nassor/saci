//! [`CsvTransformer`]: the `csv` format, both surfaces.

use std::io::{BufWriter, Write};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_csv::{ReaderBuilder, WriterBuilder};
use arrow_schema::Schema;
use arrow_select::concat::concat_batches;

use saci_core::error::SaciError;
use saci_transformer::{
    BatchReader, BatchWriter, ConfigValue, MessageDecoder, MessageShape, Transformer,
    TransformerFactory,
};

/// Whether a header row is read and written by default.
const DEFAULT_HAS_HEADERS: bool = true;

/// The `csv` byte format.
///
/// CSV carries no schema, so reading needs a declared one and the declared
/// types govern the columns whatever the text looks like.
///
/// `has_headers` governs the **stream** surface alone: it skips a first row of
/// column names while reading and emits one while writing. The message surface
/// has no header row at all, in either direction, whatever the option says. A
/// payload is one record, which leaves no room for a line of column names, and
/// [`open_message_decoder`](Transformer::open_message_decoder) is handed the
/// declared schema, so it needs none. A stream option that changed message
/// framing would make one topic readable only by a consumer that guessed the
/// producer's option.
pub struct CsvTransformer {
    has_headers: bool,
}

impl CsvTransformer {
    /// Build the format with an explicit header setting, which applies to the
    /// stream surface only.
    pub fn new(has_headers: bool) -> Self {
        Self { has_headers }
    }
}

impl Default for CsvTransformer {
    fn default() -> Self {
        Self::new(DEFAULT_HAS_HEADERS)
    }
}

impl Transformer for CsvTransformer {
    fn format(&self) -> &'static str {
        "csv"
    }

    fn open_reader(
        &self,
        input: std::fs::File,
        declared: Option<Arc<Schema>>,
    ) -> Result<Box<dyn BatchReader>, SaciError> {
        let schema = declared.ok_or_else(|| {
            SaciError::configuration("csv: reading needs a declared schema; add schema_fields")
        })?;
        // `build` wraps the handle in its own `BufReader`.
        let reader = ReaderBuilder::new(Arc::clone(&schema))
            .with_header(self.has_headers)
            .build(input)
            .map_err(|e| SaciError::generic(format!("csv: reader build failed: {e}")))?;
        Ok(Box::new(CsvBatchReader { reader, schema }))
    }

    fn open_writer(
        &self,
        output: Box<dyn Write + Send>,
        schema: Arc<Schema>,
    ) -> Result<Box<dyn BatchWriter>, SaciError> {
        let _ = schema;
        let writer = WriterBuilder::new()
            .with_header(self.has_headers)
            .build(BufWriter::new(output));
        Ok(Box::new(CsvBatchWriter { writer }))
    }

    /// Open a decoder for discrete payloads, one record apiece.
    ///
    /// The header setting stops at the stream surface: this decoder never
    /// treats a payload's first row as column names, because `schema` already
    /// names every column and a one-record payload has no line to spare.
    fn open_message_decoder(
        &self,
        schema: Arc<Schema>,
    ) -> Result<Box<dyn MessageDecoder>, SaciError> {
        let decoder = ReaderBuilder::new(Arc::clone(&schema))
            .with_header(false)
            .build_decoder();
        Ok(Box::new(CsvMessageDecoder {
            decoder,
            schema,
            pending: Vec::new(),
        }))
    }

    /// Encode one payload per row: one CSV record, no header line and no
    /// record terminator.
    ///
    /// Each row is written through its own writer rather than splitting one
    /// encoding of the batch on newlines, because a quoted field may carry a
    /// newline of its own and a split would cut the record in half.
    fn encode_messages(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>, SaciError> {
        let mut payloads = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let mut writer = WriterBuilder::new()
                .with_header(false)
                .build(Vec::with_capacity(64));
            writer
                .write(&batch.slice(row, 1))
                .map_err(|e| SaciError::generic(format!("csv: encode error: {e}")))?;
            let mut payload = writer.into_inner();
            // The writer terminates every record it writes; a payload is one
            // record and the transport delimits it, so the terminator comes
            // back off. `WriterBuilder`'s default terminator is `LF`.
            if payload.last() == Some(&b'\n') {
                payload.pop();
            }
            payloads.push(payload);
        }
        Ok(payloads)
    }

    fn message_shape(&self) -> Option<MessageShape> {
        Some(MessageShape::PerRow)
    }
}

/// Factory for [`CsvTransformer`].
///
/// `options`:
/// - `has_headers` (bool, optional, default `true`), written
///   `options has_headers=#false`. Applies to the stream surface only; the
///   message surface has no header row.
pub struct CsvTransformerFactory;

impl TransformerFactory for CsvTransformerFactory {
    fn format_name(&self) -> &'static str {
        "csv"
    }

    fn build(&self, options: &ConfigValue) -> Result<Arc<dyn Transformer>, SaciError> {
        let has_headers = match options.get("has_headers") {
            None => DEFAULT_HAS_HEADERS,
            Some(value) => value.as_bool().ok_or_else(|| {
                SaciError::configuration("csv: option 'has_headers' must be a boolean")
            })?,
        };
        Ok(Arc::new(CsvTransformer::new(has_headers)))
    }
}

struct CsvBatchReader {
    reader: arrow_csv::Reader<std::fs::File>,
    schema: Arc<Schema>,
}

impl BatchReader for CsvBatchReader {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        match self.reader.next() {
            None => Ok(None),
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => Err(SaciError::generic(format!("csv: read error: {e}"))),
        }
    }
}

struct CsvBatchWriter {
    writer: arrow_csv::Writer<BufWriter<Box<dyn Write + Send>>>,
}

impl BatchWriter for CsvBatchWriter {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        self.writer
            .write(batch)
            .map_err(|e| SaciError::generic(format!("csv: write error: {e}")))
    }

    fn finish(self: Box<Self>) -> Result<(), SaciError> {
        // CSV has no trailer, but the `BufWriter` this writer owns does hold
        // bytes: recover it and flush explicitly rather than leaving the last
        // block to `Drop`, which swallows the error.
        let mut buffered = self.writer.into_inner();
        buffered
            .flush()
            .map_err(|e| SaciError::generic(format!("csv: finish error: {e}")))
    }
}

/// One CSV record per payload, folded into one batch per window.
struct CsvMessageDecoder {
    decoder: arrow_csv::reader::Decoder,
    schema: Arc<Schema>,
    pending: Vec<RecordBatch>,
}

impl CsvMessageDecoder {
    /// Take whatever the decoder holds and park it, so a window larger than
    /// the decoder's own batch size still comes back as one batch.
    fn bank(&mut self) -> Result<(), SaciError> {
        if let Some(batch) = self
            .decoder
            .flush()
            .map_err(|e| SaciError::generic(format!("csv: flush: {e}")))?
        {
            self.pending.push(batch);
        }
        Ok(())
    }
}

impl MessageDecoder for CsvMessageDecoder {
    fn push(&mut self, payload: &[u8]) -> Result<(), SaciError> {
        // A record is complete only once the decoder sees its terminator, and a
        // payload carries none of its own. A producer that left one on is
        // normalised here rather than turning into a blank record, which the
        // decoder would report as the wrong number of fields.
        let body = payload.strip_suffix(b"\n").unwrap_or(payload);
        let body = body.strip_suffix(b"\r").unwrap_or(body);
        if body.is_empty() {
            return Err(SaciError::generic("csv: empty payload"));
        }
        for chunk in [body, b"\n".as_slice()] {
            let mut remaining = chunk;
            while !remaining.is_empty() {
                // The decoder stops at its batch capacity and then consumes
                // nothing, so bank the full batch and keep going: one window is
                // one batch to the connector whatever the decoder's batch size
                // is.
                if self.decoder.capacity() == 0 {
                    self.bank()?;
                }
                let read = self
                    .decoder
                    .decode(remaining)
                    .map_err(|e| SaciError::generic(format!("csv: decode: {e}")))?;
                if read == 0 {
                    return Err(SaciError::generic("csv: payload is not a complete record"));
                }
                remaining = &remaining[read..];
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        self.bank()?;
        match self.pending.len() {
            0 => Ok(None),
            1 => Ok(self.pending.pop()),
            _ => {
                // Every banked batch was parsed against the declared schema, so
                // they all share it.
                let folded = concat_batches(&self.schema, self.pending.iter())
                    .map_err(|e| SaciError::generic(format!("csv: concatenating batches: {e}")))?;
                self.pending.clear();
                Ok(Some(folded))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Seek;
    use std::sync::Arc;

    use arrow_array::{Float64Array, Int64Array, StringArray, UInt64Array};
    use arrow_schema::{DataType, Field};
    use tempfile::NamedTempFile;

    use super::*;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
        ]))
    }

    fn write_csv(header: bool, rows: &[&str]) -> NamedTempFile {
        let mut f = NamedTempFile::new().expect("temp file");
        if header {
            writeln!(f, "id,score").expect("header");
        }
        for row in rows {
            writeln!(f, "{row}").expect("row");
        }
        f.flush().expect("flush");
        f
    }

    fn read_all(transformer: &CsvTransformer, file: std::fs::File) -> Vec<RecordBatch> {
        let mut reader = transformer
            .open_reader(file, Some(schema()))
            .expect("reader opens");
        let mut batches = Vec::new();
        while let Some(batch) = reader.next_batch().expect("read") {
            batches.push(batch);
        }
        batches
    }

    #[test]
    fn a_header_row_is_skipped_when_has_headers_is_set() {
        let f = write_csv(true, &["1,1.5", "2,2.5", "3,3.5"]);
        let batches = read_all(
            &CsvTransformer::new(true),
            f.reopen().expect("reopen for read"),
        );
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    }

    #[test]
    fn every_row_is_data_when_has_headers_is_clear() {
        let f = write_csv(false, &["10,0.1", "20,0.2"]);
        let batches = read_all(
            &CsvTransformer::new(false),
            f.reopen().expect("reopen for read"),
        );
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);
        let scores = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float column");
        assert!((scores.value(0) - 0.1).abs() < 1e-9);
    }

    #[test]
    fn reading_without_a_declared_schema_is_a_configuration_error() {
        let f = write_csv(true, &["1,1.5"]);
        let Err(err) = CsvTransformer::default().open_reader(f.reopen().expect("reopen"), None)
        else {
            panic!("csv cannot infer a schema");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("schema_fields"), "got: {err}");
    }

    #[test]
    fn a_write_then_read_round_trip_preserves_every_row() {
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from_iter_values(0i64..4)),
                Arc::new(Float64Array::from(vec![1.1f64, 2.2, 3.3, 4.4])),
            ],
        )
        .expect("batch");

        let transformer = CsvTransformer::new(true);
        let mut file = NamedTempFile::new().expect("temp file");
        let mut writer = transformer
            .open_writer(Box::new(file.reopen().expect("reopen for write")), schema())
            .expect("writer opens");
        writer.write_batch(&batch).expect("write");
        writer.finish().expect("finish flushes");

        file.rewind().expect("rewind");
        let batches = read_all(&transformer, file.reopen().expect("reopen for read"));
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
    }

    #[test]
    fn the_factory_reads_has_headers_off_its_options_table() {
        let options = saci_transformer::from_kdl_str("has_headers #false").expect("parse kdl");
        let transformer = CsvTransformerFactory.build(&options).expect("build");
        assert_eq!(transformer.format(), "csv");
        assert_eq!(CsvTransformerFactory.format_name(), "csv");

        // A no-header write emits no name row, so the round trip through a
        // has_headers=#true read would lose the first record.
        let f = write_csv(false, &["7,0.7"]);
        let mut reader = transformer
            .open_reader(f.reopen().expect("reopen"), Some(schema()))
            .expect("reader opens");
        let batch = reader.next_batch().expect("read").expect("one batch");
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn a_non_boolean_has_headers_is_a_configuration_error() {
        let options = saci_transformer::from_kdl_str("has_headers \"yes\"").expect("parse kdl");
        let Err(err) = CsvTransformerFactory.build(&options) else {
            panic!("has_headers must be a boolean");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("has_headers"), "got: {err}");
    }

    fn decode_window(
        transformer: &CsvTransformer,
        declared: Arc<Schema>,
        payloads: &[Vec<u8>],
    ) -> Option<RecordBatch> {
        let mut decoder = transformer
            .open_message_decoder(declared)
            .expect("decoder opens");
        for payload in payloads {
            decoder.push(payload).expect("push");
        }
        decoder.flush().expect("flush")
    }

    #[test]
    fn the_message_shape_is_one_payload_per_row() {
        assert_eq!(
            CsvTransformer::default().message_shape(),
            Some(MessageShape::PerRow)
        );
    }

    #[test]
    fn encoding_emits_one_payload_per_row_carrying_no_header_and_no_terminator() {
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2])),
                Arc::new(Float64Array::from(vec![1.5f64, 2.5])),
            ],
        )
        .expect("batch");
        // `has_headers` is set, and the message surface still emits none.
        let payloads = CsvTransformer::new(true)
            .encode_messages(&batch)
            .expect("encode");
        assert_eq!(payloads, vec![b"1,1.5".to_vec(), b"2,2.5".to_vec()]);
    }

    #[test]
    fn a_payloads_first_row_is_data_even_when_has_headers_is_set() {
        let decoded = decode_window(&CsvTransformer::new(true), schema(), &[b"9,0.5".to_vec()])
            .expect("one batch");
        assert_eq!(decoded.num_rows(), 1);
        assert_eq!(
            decoded
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int column")
                .value(0),
            9
        );
    }

    #[test]
    fn a_message_round_trip_preserves_every_row_and_column_type() {
        let declared = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
            Field::new("total", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&declared),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Float64Array::from(vec![3.0f64, 5.0, 7.5])),
            ],
        )
        .expect("batch");

        let transformer = CsvTransformer::default();
        let payloads = transformer.encode_messages(&batch).expect("encode");
        assert_eq!(payloads.len(), batch.num_rows());
        let decoded =
            decode_window(&transformer, Arc::clone(&declared), &payloads).expect("one batch");
        assert_eq!(decoded, batch);
    }

    #[test]
    fn an_unsigned_column_survives_the_message_round_trip_unwidened() {
        let declared = Arc::new(Schema::new(vec![Field::new(
            "seq",
            DataType::UInt64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&declared),
            vec![Arc::new(UInt64Array::from(vec![
                1u64,
                2,
                u64::from(u32::MAX) + 7,
            ]))],
        )
        .expect("batch");

        let transformer = CsvTransformer::default();
        let payloads = transformer.encode_messages(&batch).expect("encode");
        let decoded =
            decode_window(&transformer, Arc::clone(&declared), &payloads).expect("one batch");
        assert_eq!(decoded.schema_ref().field(0).data_type(), &DataType::UInt64);
        assert_eq!(decoded, batch);
    }

    #[test]
    fn an_empty_string_comes_back_null_because_csv_writes_both_as_nothing() {
        let declared = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&declared),
            vec![
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(StringArray::from(vec![Some("")])),
            ],
        )
        .expect("batch");

        let transformer = CsvTransformer::default();
        let payloads = transformer.encode_messages(&batch).expect("encode");
        assert_eq!(payloads, vec![b"1,".to_vec()]);
        let decoded =
            decode_window(&transformer, Arc::clone(&declared), &payloads).expect("one batch");
        assert_eq!(
            decoded.column(1).logical_null_count(),
            1,
            "an empty CSV field is a null, whatever was written"
        );
    }

    #[test]
    fn a_payload_carrying_no_record_at_all_is_refused() {
        let mut decoder = CsvTransformer::default()
            .open_message_decoder(schema())
            .expect("decoder opens");
        let Err(err) = decoder.push(b"") else {
            panic!("an empty payload carries no record");
        };
        assert!(err.message().contains("empty payload"), "got: {err}");
    }

    #[test]
    fn a_payload_carrying_its_own_terminator_decodes_to_one_row() {
        let decoded = decode_window(
            &CsvTransformer::default(),
            schema(),
            &[b"4,0.25\r\n".to_vec()],
        )
        .expect("one batch");
        assert_eq!(decoded.num_rows(), 1);
    }

    #[test]
    fn a_quoted_newline_stays_inside_one_payload() {
        let declared = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&declared),
            vec![
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(StringArray::from(vec!["two\nlines"])),
            ],
        )
        .expect("batch");

        let transformer = CsvTransformer::default();
        let payloads = transformer.encode_messages(&batch).expect("encode");
        assert_eq!(payloads.len(), 1);
        let decoded = decode_window(&transformer, declared, &payloads).expect("one batch");
        assert_eq!(decoded, batch);
    }

    #[test]
    fn a_window_wider_than_the_decoders_batch_size_folds_into_one_batch() {
        let declared = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        // The decoder's default batch size is 1024, so this window spans two of
        // its internal batches.
        let payloads: Vec<Vec<u8>> = (0i64..1030).map(|i| i.to_string().into_bytes()).collect();
        let decoded =
            decode_window(&CsvTransformer::default(), declared, &payloads).expect("one batch");
        assert_eq!(decoded.num_rows(), 1030);
        let ids = decoded
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int column");
        assert_eq!(ids.value(0), 0);
        assert_eq!(ids.value(1029), 1029);
    }
}
