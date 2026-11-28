//! [`AvroCodec`]: the `avro` format, both surfaces.

use std::collections::HashMap;
use std::io::{BufReader, BufWriter, Write};
use std::sync::{Arc, OnceLock};

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
use uuid::Uuid;

use super::registry::{
    CompatibilityMode, RegistryAuth, RegistrySchemaId, SchemaRegistryClient, SchemaRegistryConfig,
    SchemaRegistryKind,
};
use crate::codec::registry::reject_unknown_options;
use crate::codec::{
    BatchReader, BatchWriter, Codec, CodecFactory, ConfigValue, CorruptRecord, MessageDecoder,
    MessageShape, ReadInput,
};
use crate::error::SaciError;
use crate::io::cast_batch;
use crate::schema::{EvolutionPolicy, evolve_batch};

/// The block compression an Avro object container file is written with.
///
/// A reader needs no setting: the codec is recorded in the file header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AvroCompression {
    /// Deflate (RFC 1951).
    Deflate,
    /// Snappy.
    Snappy,
    /// Zstandard.
    Zstd,
}

impl AvroCompression {
    /// The `arrow-avro` block codec this setting selects.
    fn block_codec(self) -> CompressionCodec {
        match self {
            Self::Deflate => CompressionCodec::Deflate,
            Self::Snappy => CompressionCodec::Snappy,
            Self::Zstd => CompressionCodec::ZStandard,
        }
    }
}

/// The `avro` byte format.
#[derive(Debug, Default)]
pub struct AvroCodec {
    compression: Option<AvroCompression>,
    schema_id: Option<u32>,
    registry_config: Option<SchemaRegistryConfig>,
    registry_client: OnceLock<Arc<SchemaRegistryClient>>,
}

impl AvroCodec {
    /// Build the format: uncompressed container files, and messages written
    /// in single-object framing.
    pub fn new() -> Self {
        Self::default()
    }

    /// Compress the blocks of the object container file the writer produces.
    /// Reading needs no setting.
    pub fn compression(mut self, compression: AvroCompression) -> Self {
        self.compression = Some(compression);
        self
    }

    /// Set the Confluent registry id: messages are written under it and, on
    /// the read side, it is the one Confluent id that resolves.
    pub fn schema_id(mut self, schema_id: u32) -> Self {
        self.schema_id = Some(schema_id);
        self
    }

    /// Attach an external schema registry configuration (Confluent, Apicurio,
    /// or AWS Glue) for dynamic writer-schema resolution and write-side
    /// schema registration.
    pub fn schema_registry(mut self, config: SchemaRegistryConfig) -> Self {
        self.registry_config = Some(config);
        self.registry_client = OnceLock::new();
        self
    }

    fn registry_client(&self) -> Result<Option<Arc<SchemaRegistryClient>>, SaciError> {
        let Some(cfg) = &self.registry_config else {
            return Ok(None);
        };
        if let Some(client) = self.registry_client.get() {
            return Ok(Some(Arc::clone(client)));
        }
        let client = Arc::new(SchemaRegistryClient::new(cfg.clone())?);
        let _ = self.registry_client.set(Arc::clone(&client));
        Ok(Some(Arc::clone(
            self.registry_client.get().unwrap_or(&client),
        )))
    }
}

impl Codec for AvroCodec {
    fn format(&self) -> &'static str {
        "avro"
    }

    fn open_reader(
        &self,
        input: ReadInput,
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
            .with_compression(self.compression.map(AvroCompression::block_codec))
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
        let registry = self.registry_client()?;
        // No decoder is built here: a consumer opens one per window, so the
        // framing that never arrives costs nothing.
        Ok(Box::new(AvroMessageDecoder {
            avro,
            schema_id: self.schema_id,
            registry,
            declared: schema,
            single_object: None,
            confluent: None,
            dynamic_id32: HashMap::new(),
            dynamic_id64: HashMap::new(),
            dynamic_uuid: HashMap::new(),
            active: None,
            pending: Vec::new(),
            skip_corrupt: false,
            corrupt: Vec::new(),
        }))
    }

    fn encode_messages(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>, SaciError> {
        let registry_id = if let Some(client) = self.registry_client()? {
            let avro_schema = AvroSchema::try_from(batch.schema().as_ref()).map_err(|e| {
                SaciError::configuration(format!("avro: batch schema has no Avro form: {e}"))
            })?;
            Some(client.resolve_or_register_for_write(&avro_schema.json_string)?)
        } else {
            self.schema_id.map(RegistrySchemaId::Id32)
        };

        let mut builder = WriterBuilder::new(batch.schema().as_ref().clone());
        match registry_id {
            Some(RegistrySchemaId::Id32(id)) => {
                builder = builder.with_fingerprint_strategy(FingerprintStrategy::Id(id));
            }
            Some(RegistrySchemaId::Id64(_)) | Some(RegistrySchemaId::GlueUuid(_)) => {
                // Encode with a dummy 5-byte Id(0) prefix so we can swap in the
                // 9-byte Apicurio or 18-byte Glue wire header cleanly per record.
                builder = builder.with_fingerprint_strategy(FingerprintStrategy::Id(0));
            }
            None => {}
        }
        // `AvroSoeFormat` is the only choice: an OCF format is rejected here,
        // and the prefix-free binary format has no public decoder.
        let mut encoder = builder
            .build_encoder::<AvroSoeFormat>()
            .map_err(|e| SaciError::generic(format!("avro: encoder build failed: {e}")))?;
        encoder
            .encode(batch)
            .map_err(|e| SaciError::generic(format!("avro: encode error: {e}")))?;
        let raw_rows = encoder.flush();
        match registry_id {
            Some(reg_id @ (RegistrySchemaId::Id64(_) | RegistrySchemaId::GlueUuid(_))) => {
                let prefix = reg_id.prefix_bytes();
                Ok(raw_rows
                    .iter()
                    .map(|row| {
                        let body = row.get(5..).unwrap_or(row.as_ref());
                        let mut out = Vec::with_capacity(prefix.len() + body.len());
                        out.extend_from_slice(&prefix);
                        out.extend_from_slice(body);
                        out
                    })
                    .collect())
            }
            _ => Ok(raw_rows.iter().map(|row| row.to_vec()).collect()),
        }
    }

    fn message_shape(&self) -> Option<MessageShape> {
        Some(MessageShape::PerRow)
    }

    // The Avro specification's own HTTP media type; no IANA registration
    // exists for the container file.
    fn media_type(&self) -> Option<&'static str> {
        Some("avro/binary")
    }
}

/// Factory for [`AvroCodec`].
///
/// `config`:
/// - `compression` (string, optional, default `"null"`): the object container
///   file block codec, one of `null`, `deflate`, `snappy`, `zstd`.
/// - `schema_id` (integer, optional, default absent): the Confluent registry
///   id. Set, messages are written Confluent-framed under it and payloads
///   carrying it decode; absent, messages are written with single-object
///   encoding and a Confluent payload is rejected. Single-object payloads
///   decode either way.
///
/// Both sit on one node: `config compression="zstd" schema_id=42`.
pub struct AvroCodecFactory;

const AVRO_OPTIONS: &[&str] = &[
    "compression",
    "schema_id",
    "registry_url",
    "registry_kind",
    "subject",
    "auto_register",
    "compatibility",
    "apicurio_group",
    "apicurio_eight_byte_id",
    "glue_registry_name",
    "glue_schema_name",
    "aws_region",
    "auth_basic_user",
    "auth_basic_password",
    "auth_bearer_token",
];

fn opt_str<'a>(options: &'a ConfigValue, key: &str) -> Result<Option<&'a str>, SaciError> {
    match options.get(key) {
        None => Ok(None),
        Some(v) => v.as_str().map(Some).ok_or_else(|| {
            SaciError::configuration(format!("avro: option '{key}' must be a string"))
        }),
    }
}

fn opt_bool(options: &ConfigValue, key: &str) -> Result<Option<bool>, SaciError> {
    match options.get(key) {
        None => Ok(None),
        Some(v) => v.as_bool().map(Some).ok_or_else(|| {
            SaciError::configuration(format!("avro: option '{key}' must be a boolean"))
        }),
    }
}

impl CodecFactory for AvroCodecFactory {
    fn format_name(&self) -> &'static str {
        "avro"
    }

    fn build(&self, options: &ConfigValue) -> Result<Arc<dyn Codec>, SaciError> {
        reject_unknown_options("avro", options, AVRO_OPTIONS)?;
        let mut codec = AvroCodec::new();
        if let Some(raw) = opt_str(options, "compression")? {
            match raw {
                "null" => {}
                "deflate" => codec = codec.compression(AvroCompression::Deflate),
                "snappy" => codec = codec.compression(AvroCompression::Snappy),
                "zstd" => codec = codec.compression(AvroCompression::Zstd),
                _ => {
                    return Err(SaciError::configuration(
                        "avro: option 'compression' must be one of null, deflate, \
                         snappy, zstd",
                    ));
                }
            }
        }
        if let Some(value) = options.get("schema_id") {
            let raw = value.as_i64().ok_or_else(|| {
                SaciError::configuration("avro: option 'schema_id' must be an integer")
            })?;
            codec = codec.schema_id(u32::try_from(raw).map_err(|_| {
                SaciError::configuration("avro: option 'schema_id' must fit in a u32")
            })?);
        }

        if let Some(endpoint) = opt_str(options, "registry_url")? {
            let kind = match opt_str(options, "registry_kind")? {
                Some(raw) => SchemaRegistryKind::from_name(raw)?,
                None => SchemaRegistryKind::Confluent,
            };
            let mut reg_cfg = match kind {
                SchemaRegistryKind::Confluent => SchemaRegistryConfig::confluent(endpoint),
                SchemaRegistryKind::Apicurio => SchemaRegistryConfig::apicurio(endpoint),
                SchemaRegistryKind::Glue => {
                    let region = opt_str(options, "aws_region")?.unwrap_or("us-east-1");
                    SchemaRegistryConfig::glue(endpoint, region)
                }
            };
            if let Some(subject) = opt_str(options, "subject")? {
                reg_cfg = reg_cfg.with_subject(subject);
            }
            if let Some(auto_register) = opt_bool(options, "auto_register")? {
                reg_cfg = reg_cfg.with_auto_register(auto_register);
            }
            if let Some(compat) = opt_str(options, "compatibility")? {
                reg_cfg = reg_cfg.with_compatibility(CompatibilityMode::from_name(compat)?);
            }
            if let Some(group) = opt_str(options, "apicurio_group")? {
                reg_cfg = reg_cfg.with_apicurio_group(group);
            }
            if let Some(eight_byte) = opt_bool(options, "apicurio_eight_byte_id")? {
                reg_cfg = reg_cfg.with_apicurio_eight_byte_id(eight_byte);
            }
            if let Some(glue_reg) = opt_str(options, "glue_registry_name")? {
                reg_cfg.glue_registry_name = glue_reg.to_string();
            }
            if let Some(glue_schema) = opt_str(options, "glue_schema_name")? {
                reg_cfg.glue_schema_name = Some(glue_schema.to_string());
            }
            if let Some(token) = opt_str(options, "auth_bearer_token")? {
                reg_cfg = reg_cfg.with_auth(RegistryAuth::Bearer {
                    token: token.to_string(),
                });
            } else if let Some(user) = opt_str(options, "auth_basic_user")? {
                let password = opt_str(options, "auth_basic_password")?.unwrap_or("");
                reg_cfg = reg_cfg.with_auth(RegistryAuth::Basic {
                    username: user.to_string(),
                    password: password.to_string(),
                });
            } else if kind == SchemaRegistryKind::Glue
                && let Some(region) = opt_str(options, "aws_region")?
            {
                reg_cfg = reg_cfg.with_auth(RegistryAuth::AwsSigV4 {
                    access_key_id: None,
                    secret_access_key: None,
                    session_token: None,
                    region: region.to_string(),
                });
            }
            // Validate registry configuration eagerly at factory build time.
            let client = Arc::new(SchemaRegistryClient::new(reg_cfg.clone())?);
            codec = codec.schema_registry(reg_cfg);
            let _ = codec.registry_client.set(client);
        }
        Ok(Arc::new(codec))
    }
}

/// Project `batch` onto `schema`.
///
/// Columns the batch carries and `schema` does not are dropped; a column
/// `schema` requires and the batch lacks is an error. Identical fields skip the
/// rebuild. `safe: false`: a value that does not fit the declared type is an
/// error, never a silent null.
fn project(batch: RecordBatch, schema: &Arc<Schema>) -> Result<RecordBatch, SaciError> {
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
    reader: arrow_avro::reader::Reader<BufReader<ReadInput>>,
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
            // the `field` entries declared, and may carry columns the projection
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

    fn flush(&mut self) -> Result<(), SaciError> {
        // `finish` here is only `BufWriter::flush`; the container has no
        // trailer.
        self.writer
            .finish()
            .map_err(|e| SaciError::generic(format!("avro: flush error: {e}")))
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
    /// `0x00` then a 4-byte big-endian static registry id (`schema_id`).
    Confluent,
    /// `0x00` then a 4-byte big-endian dynamic registry id fetched from the registry.
    DynamicId32(u32),
    /// `0x00` then an 8-byte big-endian Apicurio global/content id.
    DynamicId64(u64),
    /// `0x03 0x00` then a 16-byte AWS Glue schema version UUID.
    GlueUuid(Uuid),
}

impl Framing {
    fn label(self) -> &'static str {
        match self {
            Self::SingleObject => "single-object encoded",
            Self::Confluent | Self::DynamicId32(_) => "Confluent-framed",
            Self::DynamicId64(_) => "Apicurio 8-byte framed",
            Self::GlueUuid(_) => "AWS Glue framed",
        }
    }

    fn is_registry_dynamic(self) -> bool {
        matches!(
            self,
            Self::DynamicId32(_) | Self::DynamicId64(_) | Self::GlueUuid(_)
        )
    }
}

/// A decoder over a mixed stream of framed Avro records.
///
/// `arrow_avro::reader::Decoder` binds one fingerprint algorithm for its whole
/// life, so this keeps one decoder per framing/schema ID, picks the framing
/// off each payload's prefix bytes, and banks + adapts the outgoing decoder's
/// rows before switching so `pending` stays in arrival order.
struct AvroMessageDecoder {
    /// The declared schema in Avro form. Each static store build clones it.
    avro: AvroSchema,
    schema_id: Option<u32>,
    registry: Option<Arc<SchemaRegistryClient>>,
    declared: Arc<Schema>,
    single_object: Option<arrow_avro::reader::Decoder>,
    confluent: Option<arrow_avro::reader::Decoder>,
    dynamic_id32: HashMap<u32, arrow_avro::reader::Decoder>,
    dynamic_id64: HashMap<u64, arrow_avro::reader::Decoder>,
    dynamic_uuid: HashMap<Uuid, arrow_avro::reader::Decoder>,
    active: Option<Framing>,
    pending: Vec<RecordBatch>,
    skip_corrupt: bool,
    corrupt: Vec<CorruptRecord>,
}

impl AvroMessageDecoder {
    fn framing_of(&self, payload: &[u8]) -> Result<Framing, SaciError> {
        match payload.first().copied() {
            Some(0xC3) => Ok(Framing::SingleObject),
            Some(0x03) if payload.get(1).copied() == Some(0x00) && self.registry.is_some() => {
                if payload.len() < 18 {
                    return Err(SaciError::generic(
                        "avro: truncated AWS Glue header (expected 18 bytes)",
                    ));
                }
                let uuid = Uuid::from_slice(&payload[2..18]).map_err(|e| {
                    SaciError::generic(format!("avro: invalid Glue schema UUID: {e}"))
                })?;
                Ok(Framing::GlueUuid(uuid))
            }
            Some(0x00) if self.registry.is_some() => {
                let reg = self.registry.as_ref().expect("checked");
                if reg.config().apicurio_eight_byte_id {
                    if payload.len() < 9 {
                        return Err(SaciError::generic(
                            "avro: truncated Apicurio 8-byte header (expected 9 bytes)",
                        ));
                    }
                    let id = u64::from_be_bytes(payload[1..9].try_into().expect("8 bytes"));
                    Ok(Framing::DynamicId64(id))
                } else {
                    if payload.len() < 5 {
                        return Err(SaciError::generic(
                            "avro: truncated Confluent header (expected 5 bytes)",
                        ));
                    }
                    let id = u32::from_be_bytes(payload[1..5].try_into().expect("4 bytes"));
                    Ok(Framing::DynamicId32(id))
                }
            }
            Some(0x00) if self.schema_id.is_some() => Ok(Framing::Confluent),
            Some(0x00) => Err(SaciError::configuration(
                "avro: payload carries the Confluent prefix; set option 'schema_id' or \
                 'registry_url'",
            )),
            Some(_) => Err(SaciError::generic(
                "avro: payload is not framed; expected single-object encoding (0xC3 0x01) or the \
                 Confluent prefix (0x00)",
            )),
            None => Err(SaciError::generic("avro: empty payload")),
        }
    }

    fn slot_mut(&mut self, framing: Framing) -> Option<&mut arrow_avro::reader::Decoder> {
        match framing {
            Framing::SingleObject => self.single_object.as_mut(),
            Framing::Confluent => self.confluent.as_mut(),
            Framing::DynamicId32(id) => self.dynamic_id32.get_mut(&id),
            Framing::DynamicId64(id) => self.dynamic_id64.get_mut(&id),
            Framing::GlueUuid(uuid) => self.dynamic_uuid.get_mut(&uuid),
        }
    }

    fn ensure_decoder(&mut self, framing: Framing) -> Result<(), SaciError> {
        if self.slot_mut(framing).is_some() {
            return Ok(());
        }
        let decoder = match framing {
            Framing::SingleObject => build_static_decoder(self.avro.clone(), None)?,
            Framing::Confluent => {
                let id = self
                    .schema_id
                    .expect("Confluent framing is only reachable with a schema id");
                build_static_decoder(self.avro.clone(), Some(id))?
            }
            Framing::DynamicId32(id) => {
                let schema = self
                    .registry
                    .as_ref()
                    .expect("registry present")
                    .fetch_schema_by_id32(id)?;
                build_static_decoder((*schema).clone(), Some(id))?
            }
            Framing::DynamicId64(id) => {
                let schema = self
                    .registry
                    .as_ref()
                    .expect("registry present")
                    .fetch_schema_by_id64(id)?;
                build_static_decoder((*schema).clone(), Some(1))?
            }
            Framing::GlueUuid(uuid) => {
                let schema = self
                    .registry
                    .as_ref()
                    .expect("registry present")
                    .fetch_schema_by_uuid(uuid)?;
                build_static_decoder((*schema).clone(), Some(1))?
            }
        };
        match framing {
            Framing::SingleObject => self.single_object = Some(decoder),
            Framing::Confluent => self.confluent = Some(decoder),
            Framing::DynamicId32(id) => {
                self.dynamic_id32.insert(id, decoder);
            }
            Framing::DynamicId64(id) => {
                self.dynamic_id64.insert(id, decoder);
            }
            Framing::GlueUuid(uuid) => {
                self.dynamic_uuid.insert(uuid, decoder);
            }
        }
        Ok(())
    }

    fn bank(&mut self, framing: Framing) -> Result<(), SaciError> {
        let banked = match self.slot_mut(framing) {
            None => None,
            Some(decoder) => decoder
                .flush()
                .map_err(|e| SaciError::generic(format!("avro: flush: {e}")))?,
        };
        if let Some(batch) = banked {
            let adapted = if framing.is_registry_dynamic() {
                match evolve_batch(&batch, &self.declared, EvolutionPolicy::compatible()) {
                    Ok(evolved) => evolved,
                    Err(_) => project(batch, &self.declared)?,
                }
            } else {
                batch
            };
            self.pending.push(adapted);
        }
        Ok(())
    }
}

fn build_static_decoder(
    avro: AvroSchema,
    id: Option<u32>,
) -> Result<arrow_avro::reader::Decoder, SaciError> {
    let (mut store, fp) = match id {
        None => (SchemaStore::new(), None),
        Some(id) => (
            SchemaStore::new_with_type(FingerprintAlgorithm::Id),
            Some(Fingerprint::Id(id)),
        ),
    };
    let fingerprint = match fp {
        None => store.register(avro),
        Some(fp) => store.set(fp, avro),
    }
    .map_err(|e| SaciError::generic(format!("avro: schema registration failed: {e}")))?;
    ReaderBuilder::new()
        .with_writer_schema_store(store)
        .with_active_fingerprint(fingerprint)
        .build_decoder()
        .map_err(|e| SaciError::generic(format!("avro: decoder build failed: {e}")))
}

impl AvroMessageDecoder {
    fn drop_slot(&mut self, framing: Framing) {
        match framing {
            Framing::SingleObject => self.single_object = None,
            Framing::Confluent => self.confluent = None,
            Framing::DynamicId32(id) => {
                self.dynamic_id32.remove(&id);
            }
            Framing::DynamicId64(id) => {
                self.dynamic_id64.remove(&id);
            }
            Framing::GlueUuid(uuid) => {
                self.dynamic_uuid.remove(&uuid);
            }
        }
    }

    fn push_inner(&mut self, payload: &[u8]) -> Result<(), SaciError> {
        let framing = self.framing_of(payload)?;
        if self.active != Some(framing) {
            // Bank the framing being left so `pending` keeps arrival order
            // across a switch.
            if let Some(previous) = self.active.replace(framing) {
                self.bank(previous)?;
            }
            self.ensure_decoder(framing)?;
        } else if self.skip_corrupt {
            // When isolating bad records, bank previously decoded rows so a
            // failure inside `payload` never discards earlier valid messages.
            self.bank(framing)?;
        }
        // Normalize 9-byte Apicurio and 18-byte Glue headers into 5-byte Id(1)
        // framing that `arrow_avro::reader::Decoder` expects.
        let rewritten: Vec<u8>;
        let wire: &[u8] = match framing {
            Framing::DynamicId64(_) => {
                rewritten = [&[0x00, 0, 0, 0, 1], &payload[9..]].concat();
                &rewritten
            }
            Framing::GlueUuid(_) => {
                rewritten = [&[0x00, 0, 0, 0, 1], &payload[18..]].concat();
                &rewritten
            }
            _ => payload,
        };
        let mut remaining = wire;
        while !remaining.is_empty() {
            // The decoder stops at its batch capacity and then consumes
            // nothing, so bank the full batch and keep going: one window is one
            // batch to the connector whatever the decoder's batch size is.
            if self
                .slot_mut(framing)
                .expect("active decoder ensured")
                .batch_is_full()
            {
                self.bank(framing)?;
            }
            let read = self
                .slot_mut(framing)
                .expect("active decoder ensured")
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
}

impl MessageDecoder for AvroMessageDecoder {
    fn set_skip_corrupt(&mut self, skip: bool) {
        self.skip_corrupt = skip;
    }

    fn drain_corrupt(&mut self) -> Vec<CorruptRecord> {
        std::mem::take(&mut self.corrupt)
    }

    fn push(&mut self, payload: &[u8]) -> Result<(), SaciError> {
        if !self.skip_corrupt {
            return self.push_inner(payload);
        }
        if let Err(e) = self.push_inner(payload) {
            if let Some(active) = self.active.take() {
                self.drop_slot(active);
            }
            self.corrupt.push(CorruptRecord {
                format: "avro",
                payload: payload.to_vec(),
                error: e.to_string(),
            });
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
                let schema = self.pending[0].schema();
                let folded = concat_batches(&schema, self.pending.iter())
                    .map_err(|e| SaciError::generic(format!("avro: concatenating batches: {e}")))?;
                self.pending.clear();
                folded
            }
        };
        // Static framings may still carry Avro-widened columns; adapt or project to declared.
        if batch.schema_ref().fields() == self.declared.fields() {
            Ok(Some(batch))
        } else {
            project(batch, &self.declared).map(Some)
        }
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
        crate::codec::from_kdl_str(source).expect("parse kdl")
    }

    fn write_ocf(codec: &dyn Codec, batches: &[RecordBatch]) -> NamedTempFile {
        write_ocf_with(codec, schema(), batches)
    }

    /// The same, for a file whose own schema is not the two-column default.
    fn write_ocf_with(
        codec: &dyn Codec,
        schema: Arc<Schema>,
        batches: &[RecordBatch],
    ) -> NamedTempFile {
        let file = NamedTempFile::new().expect("temp file");
        let mut writer = codec
            .open_writer(Box::new(file.reopen().expect("reopen for write")), schema)
            .expect("writer opens");
        for batch in batches {
            writer.write_batch(batch).expect("write");
        }
        writer.finish().expect("finish flushes the last block");
        file
    }

    fn read_all(codec: &dyn Codec, file: &NamedTempFile) -> Vec<RecordBatch> {
        let mut reader = codec
            .open_reader(file.reopen().expect("reopen").into(), None)
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
        let codec = AvroCodec::default();
        let file = write_ocf(&codec, &[batch(2), batch(3)]);
        let (ids, totals) = columns(&read_all(&codec, &file));
        assert_eq!(ids, vec![0, 1, 0, 1, 2]);
        assert_eq!(totals, vec![0.0, 1.5, 0.0, 1.5, 3.0]);
    }

    #[test]
    fn the_schema_comes_from_the_file() {
        let codec = AvroCodec::default();
        let file = write_ocf(&codec, &[batch(4)]);
        let reader = codec
            .open_reader(file.reopen().expect("reopen").into(), None)
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
        let file = write_ocf_with(&AvroCodec::default(), wide_schema(), &[wide_batch(3)]);
        let mut reader = AvroCodec::default()
            .open_reader(file.reopen().expect("reopen").into(), Some(schema()))
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
        let file = write_ocf_with(&AvroCodec::default(), narrow_schema(), &[sent]);

        let mut reader = AvroCodec::default()
            .open_reader(file.reopen().expect("reopen").into(), Some(narrow_schema()))
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
        let file = write_ocf(&AvroCodec::default(), &[batch(1)]);

        let mut reader = AvroCodec::default()
            .open_reader(file.reopen().expect("reopen").into(), Some(declared))
            .expect("the container opens; the projection applies per batch");
        let Err(err) = reader.next_batch() else {
            panic!("the file carries no 'missing' column");
        };
        assert!(err.message().contains("missing"), "got: {err}");
    }

    #[test]
    fn a_run_that_writes_no_batch_still_leaves_a_readable_file() {
        let codec = AvroCodec::default();
        let file = write_ocf(&codec, &[]);
        let mut reader = codec
            .open_reader(file.reopen().expect("reopen").into(), None)
            .expect("the header alone is a readable file");
        assert!(reader.next_batch().expect("read").is_none());
    }

    #[test]
    fn a_deflate_file_round_trips() {
        let codec = AvroCodec::new().compression(AvroCompression::Deflate);
        let file = write_ocf(&codec, &[batch(64)]);
        // The compression codec comes from the header, so the reader needs none of its own.
        let (ids, _) = columns(&read_all(&AvroCodec::default(), &file));
        assert_eq!(ids, (0..64).collect::<Vec<_>>());
    }

    #[test]
    fn a_snappy_file_round_trips() {
        let codec = AvroCodec::new().compression(AvroCompression::Snappy);
        let file = write_ocf(&codec, &[batch(64)]);
        let (ids, _) = columns(&read_all(&AvroCodec::default(), &file));
        assert_eq!(ids, (0..64).collect::<Vec<_>>());
    }

    #[test]
    fn a_zstd_file_round_trips() {
        let codec = AvroCodec::new().compression(AvroCompression::Zstd);
        let file = write_ocf(&codec, &[batch(64)]);
        let (ids, _) = columns(&read_all(&AvroCodec::default(), &file));
        assert_eq!(ids, (0..64).collect::<Vec<_>>());
    }

    #[test]
    fn an_in_memory_document_reads_the_same_batches_as_a_file() {
        let codec = AvroCodec::new().compression(AvroCompression::Deflate);
        let file = write_ocf(&codec, &[batch(16), batch(8)]);
        let bytes = std::fs::read(file.path()).expect("read back");

        let from_file = read_all(&AvroCodec::new(), &file);
        let mut reader = AvroCodec::new()
            .open_reader(bytes.clone().into(), None)
            .expect("reader opens");
        let mut from_memory = Vec::new();
        while let Some(batch) = reader.next_batch().expect("read") {
            from_memory.push(batch);
        }
        assert_eq!(
            from_file.iter().map(RecordBatch::num_rows).sum::<usize>(),
            24
        );
        assert_eq!(from_file, from_memory);

        // A declared schema projects the memory variant the way it does a file.
        let projected = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let mut reader = AvroCodec::new()
            .open_reader(bytes.into(), Some(Arc::clone(&projected)))
            .expect("reader opens");
        let first = reader.next_batch().expect("read").expect("one batch");
        assert_eq!(first.schema(), projected);
    }

    #[test]
    fn the_factory_reads_compression_and_schema_id_off_its_options_table() {
        let codec = AvroCodecFactory
            .build(&options("compression \"zstd\"\nschema_id 42\n"))
            .expect("build");
        assert_eq!(codec.format(), "avro");
        assert_eq!(AvroCodecFactory.format_name(), "avro");

        // The compression codec reaches the container file header.
        let file = write_ocf(codec.as_ref(), &[batch(64)]);
        let (ids, _) = columns(&read_all(&AvroCodec::default(), &file));
        assert_eq!(ids, (0..64).collect::<Vec<_>>());

        // The registry id reaches every payload's prefix.
        let payloads = codec.encode_messages(&batch(1)).expect("encode");
        assert_eq!(payloads[0][0], 0x00);
        assert_eq!(&payloads[0][1..5], &42u32.to_be_bytes());
    }

    #[test]
    fn an_unsupported_compression_is_a_configuration_error() {
        let Err(err) = AvroCodecFactory.build(&options("compression \"bzip2\"")) else {
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
        let Err(err) = AvroCodecFactory.build(&options("compression 7")) else {
            panic!("compression must be a string");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("must be a string"), "got: {err}");
    }

    #[test]
    fn a_non_integer_schema_id_is_a_configuration_error() {
        let Err(err) = AvroCodecFactory.build(&options("schema_id \"42\"")) else {
            panic!("schema_id must be an integer");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("must be an integer"), "got: {err}");
    }

    #[test]
    fn a_schema_id_wider_than_u32_is_a_configuration_error() {
        let Err(err) = AvroCodecFactory.build(&options("schema_id 4294967296")) else {
            panic!("a Confluent registry id is a u32");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("fit in a u32"), "got: {err}");
    }

    #[test]
    fn an_option_the_format_does_not_read_is_a_configuration_error() {
        let Err(err) = AvroCodecFactory.build(&options("schema_ids 42")) else {
            panic!("a misspelled option must not be dropped");
        };
        assert_eq!(err.category(), "configuration");
        assert_eq!(err.message(), "avro: unknown option 'schema_ids'");
    }

    #[test]
    fn the_media_type_is_the_avro_specifications() {
        assert_eq!(AvroCodec::new().media_type(), Some("avro/binary"));
    }

    #[test]
    fn encoding_emits_one_message_per_row() {
        let codec = AvroCodec::default();
        assert_eq!(codec.message_shape(), Some(MessageShape::PerRow));
        let payloads = codec.encode_messages(&batch(3)).expect("encode");
        assert_eq!(payloads.len(), 3);
        for payload in &payloads {
            assert_eq!(&payload[..2], &[0xC3, 0x01], "single-object magic");
        }
    }

    #[test]
    fn a_window_of_messages_decodes_into_one_batch() {
        let codec = AvroCodec::default();
        let payloads = codec.encode_messages(&batch(3)).expect("encode");
        let mut decoder = codec.open_message_decoder(schema()).expect("decoder opens");
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
        let codec = AvroCodec::default();
        let payloads = codec.encode_messages(&batch(2_500)).expect("encode");
        let mut decoder = codec.open_message_decoder(schema()).expect("decoder opens");
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
        let codec = AvroCodec::new().schema_id(42);
        let payloads = codec.encode_messages(&batch(2)).expect("encode");
        for payload in &payloads {
            assert_eq!(payload[0], 0x00, "Confluent magic");
            assert_eq!(&payload[1..5], &42u32.to_be_bytes());
        }
        let mut decoder = codec.open_message_decoder(schema()).expect("decoder opens");
        for payload in &payloads {
            decoder.push(payload).expect("push");
        }
        let decoded = decoder.flush().expect("flush").expect("one batch");
        let (ids, _) = columns(std::slice::from_ref(&decoded));
        assert_eq!(ids, vec![0, 1]);
    }

    #[test]
    fn a_mixed_framing_window_decodes_in_arrival_order() {
        let single_object = AvroCodec::default();
        let confluent = AvroCodec::new().schema_id(7);

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
        let payload = AvroCodec::new()
            .schema_id(7)
            .encode_messages(&one_row(1))
            .expect("encode")
            .remove(0);
        let mut decoder = AvroCodec::default()
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
        let payload = AvroCodec::new()
            .schema_id(9)
            .encode_messages(&one_row(1))
            .expect("encode")
            .remove(0);
        let mut decoder = AvroCodec::new()
            .schema_id(7)
            .open_message_decoder(schema())
            .expect("decoder opens");
        let Err(err) = decoder.push(&payload) else {
            panic!("id 9 is not the id this decoder resolves");
        };
        assert!(err.message().contains("Unknown fingerprint"), "got: {err}");
    }

    #[test]
    fn a_payload_that_is_not_framed_is_a_decode_error() {
        let mut decoder = AvroCodec::default()
            .open_message_decoder(schema())
            .expect("decoder opens");
        let Err(err) = decoder.push(b"not avro") else {
            panic!("an unframed payload has no schema to decode against");
        };
        assert!(err.message().contains("not framed"), "got: {err}");
    }

    #[test]
    fn an_empty_payload_is_a_decode_error() {
        let mut decoder = AvroCodec::default()
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
        let codec = AvroCodec::default();
        let sent = RecordBatch::try_new(
            narrow_schema(),
            vec![Arc::new(Int8Array::from(vec![1i8, 2, 3]))],
        )
        .expect("batch");
        let payloads = codec.encode_messages(&sent).expect("encode");
        let mut decoder = codec
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
        let codec = AvroCodec::default();
        let payloads = codec.encode_messages(&sent).expect("encode");
        let mut decoder = codec
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

    #[test]
    fn confluent_registry_registers_and_evolves_mixed_writer_schemas_on_decode() {
        use arrow_array::Array;
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::sync::{Arc, Mutex};

        let store: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let store_srv = Arc::clone(&store);
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock registry");
        let addr = listener.local_addr().unwrap();

        std::thread::spawn(move || {
            for stream in listener.incoming().take(12) {
                let Ok(mut stream) = stream else {
                    break;
                };
                let mut buf = [0u8; 8192];
                let n = stream.read(&mut buf).unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                let req_line = req.lines().next().unwrap_or("");
                let mut parts = req_line.split_whitespace();
                let method = parts.next().unwrap_or("GET");
                let path = parts.next().unwrap_or("/");
                let body = req.split("\r\n\r\n").nth(1).unwrap_or("");

                let (status, resp_body) = if method == "POST" && path == "/subjects/ev/versions" {
                    let v: serde_json::Value = serde_json::from_str(body).unwrap();
                    let schema_str = v["schema"].as_str().unwrap().to_string();
                    let mut guard = store_srv.lock().unwrap();
                    guard.push(schema_str);
                    let id = guard.len();
                    (200, format!(r#"{{"id":{id}}}"#))
                } else if method == "GET" && path.starts_with("/schemas/ids/") {
                    let id: usize = path
                        .trim_start_matches("/schemas/ids/")
                        .parse()
                        .unwrap_or(0);
                    let guard = store_srv.lock().unwrap();
                    if let Some(s) = guard.get(id.saturating_sub(1)) {
                        (200, serde_json::json!({ "schema": s }).to_string())
                    } else {
                        (404, "{}".to_string())
                    }
                } else {
                    (404, "{}".to_string())
                };

                let reply = format!(
                    "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{resp_body}",
                    resp_body.len()
                );
                let _ = stream.write_all(reply.as_bytes());
                let _ = stream.flush();
            }
        });

        let endpoint = format!("http://{addr}");
        let writer_v1_schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let writer_v2_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("note", DataType::Utf8, true),
        ]));
        let consumer_schema = Arc::clone(&writer_v2_schema);

        let writer_codec = AvroCodec::new()
            .schema_registry(SchemaRegistryConfig::confluent(&endpoint).with_subject("ev"));

        let b1 = RecordBatch::try_new(writer_v1_schema, vec![Arc::new(Int32Array::from(vec![10]))])
            .unwrap();
        let b2 = RecordBatch::try_new(
            writer_v2_schema,
            vec![
                Arc::new(Int64Array::from(vec![20])),
                Arc::new(StringArray::from(vec![Some("v2")])),
            ],
        )
        .unwrap();

        let p1 = writer_codec
            .encode_messages(&b1)
            .expect("encode v1")
            .remove(0);
        let p2 = writer_codec
            .encode_messages(&b2)
            .expect("encode v2")
            .remove(0);
        assert_eq!(&p1[..5], &[0x00, 0, 0, 0, 1]);
        assert_eq!(&p2[..5], &[0x00, 0, 0, 0, 2]);

        let consumer_codec =
            AvroCodec::new().schema_registry(SchemaRegistryConfig::confluent(endpoint));
        let mut decoder = consumer_codec
            .open_message_decoder(Arc::clone(&consumer_schema))
            .expect("decoder opens");
        decoder.push(&p1).expect("push v1");
        decoder.push(&p2).expect("push v2");
        let decoded = decoder.flush().expect("flush").expect("batch");

        assert_eq!(decoded.schema(), consumer_schema);
        assert_eq!(decoded.num_rows(), 2);
        assert_eq!(
            decoded
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[10i64, 20]
        );
        let notes = decoded
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(notes.is_null(0));
        assert_eq!(notes.value(1), "v2");
    }
}
