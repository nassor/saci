//! [`AvroTransformer`]: the `avro` format, both surfaces.

use std::io::{BufReader, BufWriter, Write};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_avro::compression::CompressionCodec;
use arrow_avro::reader::ReaderBuilder;
use arrow_avro::schema::{
    AvroSchema, Fingerprint, FingerprintAlgorithm, FingerprintStrategy, SchemaStore,
};
use arrow_avro::writer::WriterBuilder;
use arrow_avro::writer::format::{AvroOcfFormat, AvroSoeFormat};
use arrow_cast::CastOptions;
use arrow_schema::Schema;
use arrow_select::concat::concat_batches;

use saci_core::error::SaciError;
use saci_core::io::cast_batch;
use saci_transformer::{
    BatchReader, BatchWriter, ConfigValue, MessageDecoder, MessageShape, Transformer,
    TransformerFactory,
};

/// The `avro` byte format.
#[derive(Debug, Default)]
pub struct AvroTransformer {
    compression: Option<CompressionCodec>,
    schema_id: Option<u32>,
}

impl AvroTransformer {
    /// Build the format. `compression` applies to the object container file
    /// writer; `schema_id` is the Confluent registry id, which messages are
    /// written under and, on the read side, the one Confluent id that resolves.
    pub fn new(compression: Option<CompressionCodec>, schema_id: Option<u32>) -> Self {
        Self {
            compression,
            schema_id,
        }
    }
}

impl Transformer for AvroTransformer {
    fn format(&self) -> &'static str {
        "avro"
    }

    fn open_reader(
        &self,
        input: std::fs::File,
        declared: Option<Arc<Schema>>,
    ) -> Result<Box<dyn BatchReader>, SaciError> {
        // `build` needs `BufRead`, and it reads the container header before any
        // block.
        let reader = ReaderBuilder::new()
            .build(BufReader::new(input))
            .map_err(|e| SaciError::generic(format!("avro: reader build failed: {e}")))?;
        // The file's own schema unless the config projected onto a declared
        // one: Avro stores no column subset, so a projection is applied to
        // every decoded batch rather than to the read.
        let schema = match &declared {
            Some(declared) => Arc::clone(declared),
            None => reader.schema(),
        };
        Ok(Box::new(AvroBatchReader {
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
        // The container header goes out here, so a run that writes no batch
        // still leaves a valid, readable, zero-row file.
        let writer = WriterBuilder::new(schema.as_ref().clone())
            .with_compression(self.compression)
            .build::<_, AvroOcfFormat>(BufWriter::new(output))
            .map_err(|e| SaciError::generic(format!("avro: writer init error: {e}")))?;
        Ok(Box::new(AvroBatchWriter { writer }))
    }

    fn open_message_decoder(
        &self,
        schema: Arc<Schema>,
    ) -> Result<Box<dyn MessageDecoder>, SaciError> {
        let avro = AvroSchema::try_from(schema.as_ref()).map_err(|e| {
            SaciError::configuration(format!("avro: the declared schema has no Avro form: {e}"))
        })?;
        // No decoder is built here: a consumer opens one per window, so the
        // framing that never arrives costs nothing.
        Ok(Box::new(AvroMessageDecoder {
            avro,
            schema_id: self.schema_id,
            declared: schema,
            single_object: None,
            confluent: None,
            active: None,
            pending: Vec::new(),
        }))
    }

    fn encode_messages(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>, SaciError> {
        let mut builder = WriterBuilder::new(batch.schema().as_ref().clone());
        if let Some(id) = self.schema_id {
            builder = builder.with_fingerprint_strategy(FingerprintStrategy::Id(id));
        }
        // `AvroSoeFormat` is the only choice: an OCF format is rejected here,
        // and the prefix-free binary format has no public decoder.
        let mut encoder = builder
            .build_encoder::<AvroSoeFormat>()
            .map_err(|e| SaciError::generic(format!("avro: encoder build failed: {e}")))?;
        encoder
            .encode(batch)
            .map_err(|e| SaciError::generic(format!("avro: encode error: {e}")))?;
        Ok(encoder.flush().iter().map(|row| row.to_vec()).collect())
    }

    fn message_shape(&self) -> Option<MessageShape> {
        Some(MessageShape::PerRow)
    }
}

/// Factory for [`AvroTransformer`].
///
/// `options`:
/// - `compression` (string, optional, default `"null"`): the object container
///   file block codec, one of `null`, `deflate`, `snappy`, `zstd`.
/// - `schema_id` (integer, optional, default absent): the Confluent registry
///   id. Set, messages are written Confluent-framed under it and payloads
///   carrying it decode; absent, messages are written with single-object
///   encoding and a Confluent payload is rejected. Single-object payloads
///   decode either way.
///
/// Both sit on one node: `options compression="zstd" schema_id=42`.
pub struct AvroTransformerFactory;

impl TransformerFactory for AvroTransformerFactory {
    fn format_name(&self) -> &'static str {
        "avro"
    }

    fn build(&self, options: &ConfigValue) -> Result<Arc<dyn Transformer>, SaciError> {
        let compression = match options.get("compression") {
            None => None,
            Some(value) => {
                let raw = value.as_str().ok_or_else(|| {
                    SaciError::configuration("avro: option 'compression' must be a string")
                })?;
                match raw {
                    "null" => None,
                    "deflate" => Some(CompressionCodec::Deflate),
                    "snappy" => Some(CompressionCodec::Snappy),
                    "zstd" => Some(CompressionCodec::ZStandard),
                    _ => {
                        return Err(SaciError::configuration(
                            "avro: option 'compression' must be one of null, deflate, \
                             snappy, zstd",
                        ));
                    }
                }
            }
        };
        let schema_id = match options.get("schema_id") {
            None => None,
            Some(value) => {
                let raw = value.as_i64().ok_or_else(|| {
                    SaciError::configuration("avro: option 'schema_id' must be an integer")
                })?;
                Some(u32::try_from(raw).map_err(|_| {
                    SaciError::configuration("avro: option 'schema_id' must fit in a u32")
                })?)
            }
        };
        Ok(Arc::new(AvroTransformer::new(compression, schema_id)))
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
        .map_err(|e| SaciError::generic(format!("avro: casting to the declared schema: {e}")))
}

struct AvroBatchReader {
    reader: arrow_avro::reader::Reader<BufReader<std::fs::File>>,
    /// What [`BatchReader::schema`] reports: `declared` when the config named
    /// one, the file's own otherwise.
    schema: Arc<Schema>,
    /// The projection, `None` when the file's own schema governs.
    declared: Option<Arc<Schema>>,
}

impl BatchReader for AvroBatchReader {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        match self.reader.next() {
            None => Ok(None),
            // Avro widens: int8/uint8/uint16 all arrive as Int32, so a decoded
            // batch carries the types Avro has rather than the ones
            // `schema_fields` declared, and may carry columns the projection
            // drops.
            Some(Ok(batch)) => match &self.declared {
                Some(declared) => project(batch, declared).map(Some),
                None => Ok(Some(batch)),
            },
            Some(Err(e)) => Err(SaciError::generic(format!("avro: read error: {e}"))),
        }
    }
}

struct AvroBatchWriter {
    writer: arrow_avro::writer::AvroWriter<BufWriter<Box<dyn Write + Send>>>,
}

impl BatchWriter for AvroBatchWriter {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        self.writer
            .write(batch)
            .map_err(|e| SaciError::generic(format!("avro: write error: {e}")))
    }

    fn finish(mut self: Box<Self>) -> Result<(), SaciError> {
        // The container header went out at open; `finish` only flushes, and
        // `BufWriter::flush` carries the last block down to the handle.
        self.writer
            .finish()
            .map_err(|e| SaciError::generic(format!("avro: finish error: {e}")))
    }
}

/// Which prefix a payload carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Framing {
    /// `0xC3 0x01` then an 8-byte little-endian Rabin fingerprint.
    SingleObject,
    /// `0x00` then a 4-byte big-endian registry id.
    Confluent,
}

impl Framing {
    fn label(self) -> &'static str {
        match self {
            Self::SingleObject => "single-object encoded",
            Self::Confluent => "Confluent-framed",
        }
    }
}

/// A decoder over a mixed stream of framed Avro records.
///
/// `arrow_avro::reader::Decoder` binds one fingerprint algorithm for its whole
/// life, so this keeps one decoder per framing, picks the framing off each
/// payload's first byte, and banks the outgoing decoder's rows before switching
/// so `pending` stays in arrival order.
struct AvroMessageDecoder {
    /// The declared schema in Avro form. Each store build clones it.
    avro: AvroSchema,
    schema_id: Option<u32>,
    declared: Arc<Schema>,
    single_object: Option<arrow_avro::reader::Decoder>,
    confluent: Option<arrow_avro::reader::Decoder>,
    active: Option<Framing>,
    pending: Vec<RecordBatch>,
}

impl AvroMessageDecoder {
    fn framing_of(&self, payload: &[u8]) -> Result<Framing, SaciError> {
        match payload.first().copied() {
            Some(0xC3) => Ok(Framing::SingleObject),
            Some(0x00) if self.schema_id.is_some() => Ok(Framing::Confluent),
            Some(0x00) => Err(SaciError::configuration(
                "avro: payload carries the Confluent prefix; set option 'schema_id' to its \
                 registry id",
            )),
            Some(_) => Err(SaciError::generic(
                "avro: payload is not framed; expected single-object encoding (0xC3 0x01) or the \
                 Confluent prefix (0x00)",
            )),
            None => Err(SaciError::generic("avro: empty payload")),
        }
    }

    fn slot(&mut self, framing: Framing) -> &mut Option<arrow_avro::reader::Decoder> {
        match framing {
            Framing::SingleObject => &mut self.single_object,
            Framing::Confluent => &mut self.confluent,
        }
    }

    fn decoder(&mut self, framing: Framing) -> &mut arrow_avro::reader::Decoder {
        self.slot(framing)
            .as_mut()
            .expect("the active framing's decoder is built when it becomes active")
    }

    fn build(&self, framing: Framing) -> Result<arrow_avro::reader::Decoder, SaciError> {
        let (mut store, id) = match framing {
            Framing::SingleObject => (SchemaStore::new(), None),
            Framing::Confluent => (
                SchemaStore::new_with_type(FingerprintAlgorithm::Id),
                Some(Fingerprint::Id(self.schema_id.expect(
                    "Confluent framing is only reachable with a schema id",
                ))),
            ),
        };
        let fingerprint = match id {
            None => store.register(self.avro.clone()),
            Some(id) => store.set(id, self.avro.clone()),
        }
        .map_err(|e| SaciError::generic(format!("avro: schema registration failed: {e}")))?;
        ReaderBuilder::new()
            .with_writer_schema_store(store)
            // Seeds the expected fingerprint so the first payload is not read as
            // a schema switch.
            .with_active_fingerprint(fingerprint)
            .build_decoder()
            .map_err(|e| SaciError::generic(format!("avro: decoder build failed: {e}")))
    }

    fn bank(&mut self, framing: Framing) -> Result<(), SaciError> {
        let banked = match self.slot(framing).as_mut() {
            None => None,
            Some(decoder) => decoder
                .flush()
                .map_err(|e| SaciError::generic(format!("avro: flush: {e}")))?,
        };
        if let Some(batch) = banked {
            self.pending.push(batch);
        }
        Ok(())
    }
}

impl MessageDecoder for AvroMessageDecoder {
    fn push(&mut self, payload: &[u8]) -> Result<(), SaciError> {
        let framing = self.framing_of(payload)?;
        if self.active != Some(framing) {
            // Bank the framing being left so `pending` keeps arrival order
            // across a switch.
            if let Some(previous) = self.active.replace(framing) {
                self.bank(previous)?;
            }
            if self.slot(framing).is_none() {
                let decoder = self.build(framing)?;
                *self.slot(framing) = Some(decoder);
            }
        }
        let mut remaining = payload;
        while !remaining.is_empty() {
            // The decoder stops at its batch capacity and then consumes
            // nothing, so bank the full batch and keep going: one window is one
            // batch to the connector whatever the decoder's batch size is.
            if self.decoder(framing).batch_is_full() {
                self.bank(framing)?;
            }
            let read = self
                .decoder(framing)
                .decode(remaining)
                .map_err(|e| SaciError::generic(format!("avro: decode: {e}")))?;
            if read == 0 {
                return Err(SaciError::generic(format!(
                    "avro: payload is not a complete {} record",
                    framing.label()
                )));
            }
            remaining = &remaining[read..];
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        // Only the active framing's decoder can hold rows: every switch banks
        // the one it leaves.
        if let Some(framing) = self.active {
            self.bank(framing)?;
        }
        let batch = match self.pending.len() {
            0 => return Ok(None),
            1 => self.pending.pop().expect("one banked batch"),
            _ => {
                // Both framings resolve the same Avro schema, so every banked
                // batch has the same Arrow schema and the first one's governs
                // the fold.
                let schema = self.pending[0].schema();
                let folded = concat_batches(&schema, self.pending.iter())
                    .map_err(|e| SaciError::generic(format!("avro: concatenating batches: {e}")))?;
                self.pending.clear();
                folded
            }
        };
        // Avro widens, so the decoded columns may not be the declared ones.
        project(batch, &self.declared).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Float64Array, Int8Array, Int32Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field};
    use tempfile::NamedTempFile;

    use super::*;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("total", DataType::Float64, false),
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

    /// One row carrying `id`, so an arrival order is observable.
    fn one_row(id: i64) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from_iter_values([id])),
                Arc::new(Float64Array::from_iter_values([id as f64])),
            ],
        )
        .expect("batch")
    }

    fn options(source: &str) -> ConfigValue {
        saci_transformer::from_kdl_str(source).expect("parse kdl")
    }

    fn write_ocf(transformer: &dyn Transformer, batches: &[RecordBatch]) -> NamedTempFile {
        write_ocf_with(transformer, schema(), batches)
    }

    /// The same, for a file whose own schema is not the two-column default.
    fn write_ocf_with(
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
        writer.finish().expect("finish flushes the last block");
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

    /// Every `id` then every `total`, in the order they were read.
    fn columns(batches: &[RecordBatch]) -> (Vec<i64>, Vec<f64>) {
        let mut ids = Vec::new();
        let mut totals = Vec::new();
        for batch in batches {
            ids.extend(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("id is Int64")
                    .values(),
            );
            totals.extend(
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("total is Float64")
                    .values(),
            );
        }
        (ids, totals)
    }

    #[test]
    fn a_write_then_read_round_trip_preserves_every_row() {
        let transformer = AvroTransformer::default();
        let file = write_ocf(&transformer, &[batch(2), batch(3)]);
        let (ids, totals) = columns(&read_all(&transformer, &file));
        assert_eq!(ids, vec![0, 1, 0, 1, 2]);
        assert_eq!(totals, vec![0.0, 1.5, 0.0, 1.5, 3.0]);
    }

    #[test]
    fn the_schema_comes_from_the_file() {
        let transformer = AvroTransformer::default();
        let file = write_ocf(&transformer, &[batch(4)]);
        let reader = transformer
            .open_reader(file.reopen().expect("reopen"), None)
            .expect("reader opens");
        let schema = reader.schema();
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).name(), "total");
        assert_eq!(schema.field(1).data_type(), &DataType::Float64);
    }

    /// The file's own three columns: one more than `schema` projects onto.
    fn wide_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("total", DataType::Float64, false),
            Field::new("note", DataType::Utf8, false),
        ]))
    }

    fn wide_batch(rows: i64) -> RecordBatch {
        let notes = StringArray::from_iter_values((0..rows).map(|i| format!("n{i}")));
        RecordBatch::try_new(
            wide_schema(),
            vec![
                Arc::new(Int64Array::from_iter_values(0..rows)),
                Arc::new(Float64Array::from_iter_values(
                    (0..rows).map(|i| i as f64 * 1.5),
                )),
                Arc::new(notes),
            ],
        )
        .expect("batch")
    }

    #[test]
    fn a_declared_schema_projects_a_wider_file() {
        let file = write_ocf_with(&AvroTransformer::default(), wide_schema(), &[wide_batch(3)]);
        let mut reader = AvroTransformer::default()
            .open_reader(file.reopen().expect("reopen"), Some(schema()))
            .expect("a declared schema is a projection target");

        assert_eq!(reader.schema(), schema(), "the declared schema governs");
        let batch = reader.next_batch().expect("read").expect("one batch");
        assert_eq!(batch.num_columns(), 2);
        assert!(
            batch.schema().field_with_name("note").is_err(),
            "the unprojected column is gone"
        );
        let (ids, totals) = columns(std::slice::from_ref(&batch));
        assert_eq!(ids, vec![0, 1, 2]);
        assert_eq!(totals, vec![0.0, 1.5, 3.0]);
    }

    #[test]
    fn a_declared_type_narrower_than_avro_is_cast_back_on_the_read_surface() {
        // `int8` travels as Avro `int`, so the file hands back an `Int32`.
        let sent = RecordBatch::try_new(
            narrow_schema(),
            vec![Arc::new(Int8Array::from(vec![1i8, 2, 3]))],
        )
        .expect("batch");
        let file = write_ocf_with(&AvroTransformer::default(), narrow_schema(), &[sent]);

        let mut reader = AvroTransformer::default()
            .open_reader(file.reopen().expect("reopen"), Some(narrow_schema()))
            .expect("reader opens");
        let batch = reader.next_batch().expect("read").expect("one batch");
        assert_eq!(batch.schema(), narrow_schema());
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("id narrowed back to Int8")
                .values(),
            &[1, 2, 3]
        );
    }

    #[test]
    fn a_declared_column_the_file_lacks_is_an_error_naming_it() {
        let declared = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("missing", DataType::Int64, false),
        ]));
        let file = write_ocf(&AvroTransformer::default(), &[batch(1)]);

        let mut reader = AvroTransformer::default()
            .open_reader(file.reopen().expect("reopen"), Some(declared))
            .expect("the container opens; the projection applies per batch");
        let Err(err) = reader.next_batch() else {
            panic!("the file carries no 'missing' column");
        };
        assert!(err.message().contains("missing"), "got: {err}");
    }

    #[test]
    fn a_run_that_writes_no_batch_still_leaves_a_readable_file() {
        let transformer = AvroTransformer::default();
        let file = write_ocf(&transformer, &[]);
        let mut reader = transformer
            .open_reader(file.reopen().expect("reopen"), None)
            .expect("the header alone is a readable file");
        assert!(reader.next_batch().expect("read").is_none());
    }

    #[test]
    fn a_deflate_file_round_trips() {
        let transformer = AvroTransformer::new(Some(CompressionCodec::Deflate), None);
        let file = write_ocf(&transformer, &[batch(64)]);
        // The codec comes from the header, so the reader needs none of its own.
        let (ids, _) = columns(&read_all(&AvroTransformer::default(), &file));
        assert_eq!(ids, (0..64).collect::<Vec<_>>());
    }

    #[test]
    fn a_snappy_file_round_trips() {
        let transformer = AvroTransformer::new(Some(CompressionCodec::Snappy), None);
        let file = write_ocf(&transformer, &[batch(64)]);
        let (ids, _) = columns(&read_all(&AvroTransformer::default(), &file));
        assert_eq!(ids, (0..64).collect::<Vec<_>>());
    }

    #[test]
    fn a_zstd_file_round_trips() {
        let transformer = AvroTransformer::new(Some(CompressionCodec::ZStandard), None);
        let file = write_ocf(&transformer, &[batch(64)]);
        let (ids, _) = columns(&read_all(&AvroTransformer::default(), &file));
        assert_eq!(ids, (0..64).collect::<Vec<_>>());
    }

    #[test]
    fn the_factory_reads_compression_and_schema_id_off_its_options_table() {
        let transformer = AvroTransformerFactory
            .build(&options("compression \"zstd\"\nschema_id 42\n"))
            .expect("build");
        assert_eq!(transformer.format(), "avro");
        assert_eq!(AvroTransformerFactory.format_name(), "avro");

        // The codec reaches the container file header.
        let file = write_ocf(transformer.as_ref(), &[batch(64)]);
        let (ids, _) = columns(&read_all(&AvroTransformer::default(), &file));
        assert_eq!(ids, (0..64).collect::<Vec<_>>());

        // The registry id reaches every payload's prefix.
        let payloads = transformer.encode_messages(&batch(1)).expect("encode");
        assert_eq!(payloads[0][0], 0x00);
        assert_eq!(&payloads[0][1..5], &42u32.to_be_bytes());
    }

    #[test]
    fn an_unsupported_compression_is_a_configuration_error() {
        let Err(err) = AvroTransformerFactory.build(&options("compression \"bzip2\"")) else {
            panic!("bzip2 is not a codec this build can write");
        };
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("null, deflate, snappy, zstd"),
            "got: {err}"
        );
    }

    #[test]
    fn a_non_string_compression_is_a_configuration_error() {
        let Err(err) = AvroTransformerFactory.build(&options("compression 7")) else {
            panic!("compression must be a string");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("must be a string"), "got: {err}");
    }

    #[test]
    fn a_non_integer_schema_id_is_a_configuration_error() {
        let Err(err) = AvroTransformerFactory.build(&options("schema_id \"42\"")) else {
            panic!("schema_id must be an integer");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("must be an integer"), "got: {err}");
    }

    #[test]
    fn a_schema_id_wider_than_u32_is_a_configuration_error() {
        let Err(err) = AvroTransformerFactory.build(&options("schema_id 4294967296")) else {
            panic!("a Confluent registry id is a u32");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("fit in a u32"), "got: {err}");
    }

    #[test]
    fn encoding_emits_one_message_per_row() {
        let transformer = AvroTransformer::default();
        assert_eq!(transformer.message_shape(), Some(MessageShape::PerRow));
        let payloads = transformer.encode_messages(&batch(3)).expect("encode");
        assert_eq!(payloads.len(), 3);
        for payload in &payloads {
            assert_eq!(&payload[..2], &[0xC3, 0x01], "single-object magic");
        }
    }

    #[test]
    fn a_window_of_messages_decodes_into_one_batch() {
        let transformer = AvroTransformer::default();
        let payloads = transformer.encode_messages(&batch(3)).expect("encode");
        let mut decoder = transformer
            .open_message_decoder(schema())
            .expect("decoder opens");
        for payload in &payloads {
            decoder.push(payload).expect("push");
        }
        let decoded = decoder.flush().expect("flush").expect("one batch");
        assert_eq!(decoded.num_rows(), 3);
        let (ids, totals) = columns(std::slice::from_ref(&decoded));
        assert_eq!(ids, vec![0, 1, 2]);
        assert_eq!(totals, vec![0.0, 1.5, 3.0]);
        assert!(
            decoder.flush().expect("flush").is_none(),
            "the window reset"
        );
    }

    #[test]
    fn a_window_wider_than_one_decoder_batch_decodes_into_one_batch() {
        let transformer = AvroTransformer::default();
        let payloads = transformer.encode_messages(&batch(2_500)).expect("encode");
        let mut decoder = transformer
            .open_message_decoder(schema())
            .expect("decoder opens");
        for payload in &payloads {
            decoder.push(payload).expect("push");
        }
        let decoded = decoder.flush().expect("flush").expect("one batch");
        assert_eq!(
            decoded.num_rows(),
            2_500,
            "the decoder's 1024-row batch must not truncate the window"
        );
        let (ids, _) = columns(std::slice::from_ref(&decoded));
        assert_eq!(ids, (0..2_500).collect::<Vec<_>>());
    }

    #[test]
    fn a_confluent_framed_payload_carries_the_schema_id() {
        let transformer = AvroTransformer::new(None, Some(42));
        let payloads = transformer.encode_messages(&batch(2)).expect("encode");
        for payload in &payloads {
            assert_eq!(payload[0], 0x00, "Confluent magic");
            assert_eq!(&payload[1..5], &42u32.to_be_bytes());
        }
        let mut decoder = transformer
            .open_message_decoder(schema())
            .expect("decoder opens");
        for payload in &payloads {
            decoder.push(payload).expect("push");
        }
        let decoded = decoder.flush().expect("flush").expect("one batch");
        let (ids, _) = columns(std::slice::from_ref(&decoded));
        assert_eq!(ids, vec![0, 1]);
    }

    #[test]
    fn a_mixed_framing_window_decodes_in_arrival_order() {
        let single_object = AvroTransformer::default();
        let confluent = AvroTransformer::new(None, Some(7));

        let first = single_object
            .encode_messages(&one_row(1))
            .expect("encode")
            .remove(0);
        let second = confluent
            .encode_messages(&one_row(2))
            .expect("encode")
            .remove(0);
        let third = single_object
            .encode_messages(&one_row(3))
            .expect("encode")
            .remove(0);

        let mut decoder = confluent
            .open_message_decoder(schema())
            .expect("decoder opens");
        for payload in [&first, &second, &third] {
            decoder.push(payload).expect("push");
        }
        let decoded = decoder.flush().expect("flush").expect("one batch");
        let (ids, _) = columns(std::slice::from_ref(&decoded));
        assert_eq!(ids, vec![1, 2, 3], "framing switches keep arrival order");
    }

    #[test]
    fn a_confluent_payload_without_a_schema_id_names_the_option() {
        let payload = AvroTransformer::new(None, Some(7))
            .encode_messages(&one_row(1))
            .expect("encode")
            .remove(0);
        let mut decoder = AvroTransformer::default()
            .open_message_decoder(schema())
            .expect("decoder opens");
        let Err(err) = decoder.push(&payload) else {
            panic!("a registry id cannot be resolved without one in config");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("schema_id"), "got: {err}");
    }

    #[test]
    fn an_unknown_confluent_id_is_a_decode_error() {
        let payload = AvroTransformer::new(None, Some(9))
            .encode_messages(&one_row(1))
            .expect("encode")
            .remove(0);
        let mut decoder = AvroTransformer::new(None, Some(7))
            .open_message_decoder(schema())
            .expect("decoder opens");
        let Err(err) = decoder.push(&payload) else {
            panic!("id 9 is not the id this decoder resolves");
        };
        assert!(err.message().contains("Unknown fingerprint"), "got: {err}");
    }

    #[test]
    fn a_payload_that_is_not_framed_is_a_decode_error() {
        let mut decoder = AvroTransformer::default()
            .open_message_decoder(schema())
            .expect("decoder opens");
        let Err(err) = decoder.push(b"not avro") else {
            panic!("an unframed payload has no schema to decode against");
        };
        assert!(err.message().contains("not framed"), "got: {err}");
    }

    #[test]
    fn an_empty_payload_is_a_decode_error() {
        let mut decoder = AvroTransformer::default()
            .open_message_decoder(schema())
            .expect("decoder opens");
        let Err(err) = decoder.push(b"") else {
            panic!("an empty payload carries no record");
        };
        assert!(err.message().contains("empty payload"), "got: {err}");
    }

    /// `int8` has no Avro form of its own: it travels as `int`.
    fn narrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int8, false)]))
    }

    #[test]
    fn a_declared_type_narrower_than_avro_is_cast_back() {
        let transformer = AvroTransformer::default();
        let sent = RecordBatch::try_new(
            narrow_schema(),
            vec![Arc::new(Int8Array::from(vec![1i8, 2, 3]))],
        )
        .expect("batch");
        let payloads = transformer.encode_messages(&sent).expect("encode");
        let mut decoder = transformer
            .open_message_decoder(narrow_schema())
            .expect("decoder opens");
        for payload in &payloads {
            decoder.push(payload).expect("push");
        }
        let decoded = decoder.flush().expect("flush").expect("one batch");
        assert_eq!(decoded.schema(), narrow_schema());
        assert_eq!(
            decoded
                .column(0)
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("id narrowed back to Int8")
                .values(),
            &[1, 2, 3]
        );
    }

    #[test]
    fn a_value_that_does_not_fit_the_declared_type_is_an_error() {
        // An Arrow `int32` and an Arrow `int8` share one Avro form, `int`, so
        // this payload's fingerprint resolves against the narrow declaration.
        let wide = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let sent =
            RecordBatch::try_new(wide, vec![Arc::new(Int32Array::from(vec![400]))]).expect("batch");
        let transformer = AvroTransformer::default();
        let payloads = transformer.encode_messages(&sent).expect("encode");
        let mut decoder = transformer
            .open_message_decoder(narrow_schema())
            .expect("decoder opens");
        for payload in &payloads {
            decoder.push(payload).expect("push");
        }
        let Err(err) = decoder.flush() else {
            panic!("400 does not fit an int8");
        };
        assert!(err.message().contains("casting"), "got: {err}");
    }
}
