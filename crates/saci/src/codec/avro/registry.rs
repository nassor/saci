//! External Avro schema registry clients (Confluent, Apicurio, AWS Glue),
//! compatibility checks, and thread-safe in-memory caching.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use arrow_avro::schema::AvroSchema;
use reqwest::Method;
use ring::{digest, hmac};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::error::SaciError;

/// Which schema registry wire protocol and API dialect to speak.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SchemaRegistryKind {
    /// Confluent Schema Registry REST API + 5-byte prefix (`0x00` + u32 BE).
    #[default]
    Confluent,
    /// Apicurio Registry v2 API + 5-byte or 9-byte prefix (`0x00` + u32/u64 BE).
    Apicurio,
    /// AWS Glue Schema Registry API (SigV4 JSON) + 18-byte header (`0x03 0x00` + UUID).
    Glue,
}

impl SchemaRegistryKind {
    /// Parse a registry kind string (`confluent`, `apicurio`, `glue`).
    pub(crate) fn from_name(raw: &str) -> Result<Self, SaciError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "confluent" => Ok(Self::Confluent),
            "apicurio" => Ok(Self::Apicurio),
            "glue" => Ok(Self::Glue),
            other => Err(SaciError::configuration(format!(
                "avro: unknown registry_kind '{other}'; expected confluent, apicurio or glue"
            ))),
        }
    }
}

/// Avro schema compatibility guarantees checked on registration or write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompatibilityMode {
    /// No compatibility check performed.
    #[default]
    None,
    /// New schema (reader) can read records written by the previous schema.
    Backward,
    /// New schema (reader) can read records written by all previous schemas.
    BackwardTransitive,
    /// Previous schema (reader) can read records written by the new schema.
    Forward,
    /// All previous schemas can read records written by the new schema.
    ForwardTransitive,
    /// Both backward and forward compatible against the previous schema.
    Full,
    /// Both backward and forward compatible against all previous schemas.
    FullTransitive,
}

impl CompatibilityMode {
    /// Parse a compatibility mode string.
    pub(crate) fn from_name(raw: &str) -> Result<Self, SaciError> {
        match raw.trim().to_ascii_uppercase().as_str() {
            "NONE" => Ok(Self::None),
            "BACKWARD" => Ok(Self::Backward),
            "BACKWARD_TRANSITIVE" => Ok(Self::BackwardTransitive),
            "FORWARD" => Ok(Self::Forward),
            "FORWARD_TRANSITIVE" => Ok(Self::ForwardTransitive),
            "FULL" => Ok(Self::Full),
            "FULL_TRANSITIVE" => Ok(Self::FullTransitive),
            other => Err(SaciError::configuration(format!(
                "avro: unknown compatibility '{other}'; expected NONE, BACKWARD, BACKWARD_TRANSITIVE, \
                 FORWARD, FORWARD_TRANSITIVE, FULL, or FULL_TRANSITIVE"
            ))),
        }
    }

    /// Registry API representation of the compatibility level.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Backward => "BACKWARD",
            Self::BackwardTransitive => "BACKWARD_TRANSITIVE",
            Self::Forward => "FORWARD",
            Self::ForwardTransitive => "FORWARD_TRANSITIVE",
            Self::Full => "FULL",
            Self::FullTransitive => "FULL_TRANSITIVE",
        }
    }

    /// Returns `true` when every historical version must satisfy compatibility.
    #[must_use]
    pub const fn is_transitive(self) -> bool {
        matches!(
            self,
            Self::BackwardTransitive | Self::ForwardTransitive | Self::FullTransitive
        )
    }

    /// AWS Glue Schema Registry `Compatibility` enum string.
    #[must_use]
    pub const fn glue_str(self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Backward => "BACKWARD",
            Self::BackwardTransitive => "BACKWARD_ALL",
            Self::Forward => "FORWARD",
            Self::ForwardTransitive => "FORWARD_ALL",
            Self::Full => "FULL",
            Self::FullTransitive => "FULL_ALL",
        }
    }
}

/// Authentication mechanism for an external schema registry.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum RegistryAuth {
    /// Unauthenticated HTTP(S) access.
    #[default]
    None,
    /// HTTP Basic credentials.
    Basic {
        /// Username (API key in Confluent Cloud).
        username: String,
        /// Password (API secret in Confluent Cloud).
        password: String,
    },
    /// HTTP `Authorization: Bearer <token>`.
    Bearer {
        /// Bearer token string.
        token: String,
    },
    /// AWS SigV4 request signing for Glue Schema Registry.
    AwsSigV4 {
        /// Optional static AWS access key ID; falls back to `AWS_ACCESS_KEY_ID`.
        access_key_id: Option<String>,
        /// Optional static AWS secret key; falls back to `AWS_SECRET_ACCESS_KEY`.
        secret_access_key: Option<String>,
        /// Optional STS session token; falls back to `AWS_SESSION_TOKEN`.
        session_token: Option<String>,
        /// AWS signing region (for example `us-east-1`).
        region: String,
    },
}

/// Configuration for connecting [`AvroCodec`](super::AvroCodec) to an external
/// schema registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaRegistryConfig {
    /// Registry protocol / dialect.
    pub kind: SchemaRegistryKind,
    /// Base URL (e.g. `http://localhost:8081` or `https://glue.us-east-1.amazonaws.com`).
    pub endpoint: String,
    /// Subject or artifact ID for registration / lookup on write.
    pub subject: Option<String>,
    /// Register schemas automatically on write when absent from the registry.
    pub auto_register: bool,
    /// Compatibility check mode enforced before registration/write.
    pub compatibility: CompatibilityMode,
    /// Authentication configuration.
    pub auth: RegistryAuth,
    /// Apicurio artifact group (`"default"` by default).
    pub apicurio_group: String,
    /// When `true`, Apicurio frames 8-byte global/content IDs (`0x00` + 8-byte BE u64).
    pub apicurio_eight_byte_id: bool,
    /// AWS Glue registry name (`"default-registry"` by default).
    pub glue_registry_name: String,
    /// AWS Glue schema name (defaults to `subject` if unset).
    pub glue_schema_name: Option<String>,
}

impl Default for SchemaRegistryConfig {
    fn default() -> Self {
        Self {
            kind: SchemaRegistryKind::Confluent,
            endpoint: String::new(),
            subject: None,
            auto_register: true,
            compatibility: CompatibilityMode::None,
            auth: RegistryAuth::None,
            apicurio_group: "default".to_string(),
            apicurio_eight_byte_id: false,
            glue_registry_name: "default-registry".to_string(),
            glue_schema_name: None,
        }
    }
}

impl SchemaRegistryConfig {
    /// Create a Confluent Schema Registry configuration pointing at `endpoint`.
    pub fn confluent(endpoint: impl Into<String>) -> Self {
        Self {
            kind: SchemaRegistryKind::Confluent,
            endpoint: endpoint.into().trim_end_matches('/').to_string(),
            ..Self::default()
        }
    }

    /// Create an Apicurio Registry v2 configuration pointing at `endpoint`.
    pub fn apicurio(endpoint: impl Into<String>) -> Self {
        Self {
            kind: SchemaRegistryKind::Apicurio,
            endpoint: endpoint.into().trim_end_matches('/').to_string(),
            ..Self::default()
        }
    }

    /// Create an AWS Glue Schema Registry configuration in `region`.
    pub fn glue(endpoint: impl Into<String>, region: impl Into<String>) -> Self {
        Self {
            kind: SchemaRegistryKind::Glue,
            endpoint: endpoint.into().trim_end_matches('/').to_string(),
            auth: RegistryAuth::AwsSigV4 {
                access_key_id: None,
                secret_access_key: None,
                session_token: None,
                region: region.into(),
            },
            ..Self::default()
        }
    }

    /// Set the Confluent subject / Apicurio artifact ID.
    pub fn with_subject(mut self, subject: impl Into<String>) -> Self {
        self.subject = Some(subject.into());
        self
    }

    /// Control whether unseen schemas are registered automatically on write.
    pub fn with_auto_register(mut self, auto_register: bool) -> Self {
        self.auto_register = auto_register;
        self
    }

    /// Set compatibility enforcement mode.
    pub fn with_compatibility(mut self, mode: CompatibilityMode) -> Self {
        self.compatibility = mode;
        self
    }

    /// Set HTTP / AWS authentication credentials.
    pub fn with_auth(mut self, auth: RegistryAuth) -> Self {
        self.auth = auth;
        self
    }

    /// Set the Apicurio artifact group.
    pub fn with_apicurio_group(mut self, group: impl Into<String>) -> Self {
        self.apicurio_group = group.into();
        self
    }

    /// Toggle Apicurio 8-byte global/content ID wire framing.
    pub fn with_apicurio_eight_byte_id(mut self, eight_byte: bool) -> Self {
        self.apicurio_eight_byte_id = eight_byte;
        self
    }

    /// Set AWS Glue registry and schema names.
    pub fn with_glue_names(
        mut self,
        registry_name: impl Into<String>,
        schema_name: impl Into<String>,
    ) -> Self {
        self.glue_registry_name = registry_name.into();
        self.glue_schema_name = Some(schema_name.into());
        self
    }
}

/// Wire identifier returned after registering or looking up an Avro schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RegistrySchemaId {
    /// 4-byte Confluent or Apicurio schema ID (`0x00` + `u32` BE).
    Id32(u32),
    /// 8-byte Apicurio global/content ID (`0x00` + `u64` BE).
    Id64(u64),
    /// AWS Glue schema version UUID (`0x03 0x00` + 16-byte UUID).
    GlueUuid(Uuid),
}

impl RegistrySchemaId {
    /// Write the framing header bytes for a single encoded payload.
    pub(crate) fn prefix_bytes(self) -> Vec<u8> {
        match self {
            Self::Id32(id) => {
                let mut out = Vec::with_capacity(5);
                out.push(0x00);
                out.extend_from_slice(&id.to_be_bytes());
                out
            }
            Self::Id64(id) => {
                let mut out = Vec::with_capacity(9);
                out.push(0x00);
                out.extend_from_slice(&id.to_be_bytes());
                out
            }
            Self::GlueUuid(uuid) => {
                let mut out = Vec::with_capacity(18);
                out.push(0x03);
                out.push(0x00);
                out.extend_from_slice(uuid.as_bytes());
                out
            }
        }
    }
}

/// Check local Avro schema compatibility between a `candidate_json` schema and
/// an `existing_json` schema under `mode`.
///
/// Follows Avro schema resolution semantics:
/// - `Backward*`: `candidate` (new reader) must decode records produced by `existing` (old writer).
/// - `Forward*`: `existing` (old reader) must decode records produced by `candidate` (new writer).
/// - `Full*`: both directions must succeed.
pub fn check_avro_compatibility(
    candidate_json: &str,
    existing_json: &str,
    mode: CompatibilityMode,
) -> Result<(), SaciError> {
    if mode == CompatibilityMode::None {
        return Ok(());
    }
    let candidate: Value = serde_json::from_str(candidate_json).map_err(|e| {
        SaciError::configuration(format!("avro registry: invalid candidate schema JSON: {e}"))
    })?;
    let existing: Value = serde_json::from_str(existing_json).map_err(|e| {
        SaciError::configuration(format!("avro registry: invalid existing schema JSON: {e}"))
    })?;

    match mode {
        CompatibilityMode::None => Ok(()),
        CompatibilityMode::Backward | CompatibilityMode::BackwardTransitive => {
            check_reader_writer_compat(&candidate, &existing, "backward")
        }
        CompatibilityMode::Forward | CompatibilityMode::ForwardTransitive => {
            check_reader_writer_compat(&existing, &candidate, "forward")
        }
        CompatibilityMode::Full | CompatibilityMode::FullTransitive => {
            check_reader_writer_compat(&candidate, &existing, "backward")?;
            check_reader_writer_compat(&existing, &candidate, "forward")
        }
    }
}

fn check_reader_writer_compat(reader: &Value, writer: &Value, dir: &str) -> Result<(), SaciError> {
    if avro_type_compatible(writer, reader) {
        return Ok(());
    }
    if let (Some(r_fields), Some(w_fields)) = (record_fields(reader), record_fields(writer)) {
        let mut w_map: HashMap<&str, &Value> = HashMap::with_capacity(w_fields.len());
        for f in w_fields {
            if let Some(name) = f.get("name").and_then(Value::as_str) {
                w_map.insert(name, f);
            }
        }
        for rf in r_fields {
            let name = rf.get("name").and_then(Value::as_str).unwrap_or("");
            let r_type = rf.get("type").unwrap_or(&Value::Null);
            let has_default = rf.get("default").is_some();
            match w_map.get(name) {
                Some(wf) => {
                    let w_type = wf.get("type").unwrap_or(&Value::Null);
                    check_reader_writer_compat(r_type, w_type, dir).map_err(|e| {
                        SaciError::configuration(format!(
                            "avro registry: {dir} incompatibility on field '{name}': {}",
                            e.message()
                        ))
                    })?;
                }
                None if has_default => {}
                None => {
                    return Err(SaciError::configuration(format!(
                        "avro registry: {dir} incompatibility on field '{name}': \
                         field missing in writer schema without a default value"
                    )));
                }
            }
        }
        return Ok(());
    }

    if let Some(w_union) = writer.as_array() {
        if w_union.is_empty() {
            return Err(SaciError::configuration(
                "avro registry: writer union must not be empty",
            ));
        }
        for w_branch in w_union {
            check_reader_writer_compat(reader, w_branch, dir)?;
        }
        return Ok(());
    }
    if let Some(r_union) = reader.as_array() {
        for r_branch in r_union {
            if check_reader_writer_compat(r_branch, writer, dir).is_ok() {
                return Ok(());
            }
        }
    }

    Err(SaciError::configuration(format!(
        "avro registry: {dir} type mismatch between writer {writer} and reader {reader}"
    )))
}

fn record_fields(val: &Value) -> Option<&Vec<Value>> {
    let obj = val.as_object()?;
    if obj.get("type").and_then(Value::as_str) == Some("record") {
        obj.get("fields")?.as_array()
    } else {
        None
    }
}

fn avro_primitive_name(v: &Value) -> Option<&str> {
    if let Some(s) = v.as_str() {
        return Some(s);
    }
    if let Some(obj) = v.as_object() {
        return obj.get("type").and_then(Value::as_str);
    }
    None
}

fn avro_type_compatible(writer: &Value, reader: &Value) -> bool {
    if writer == reader {
        return true;
    }
    if let (Some(w_name), Some(r_name)) = (avro_primitive_name(writer), avro_primitive_name(reader))
    {
        if w_name == r_name && !matches!(w_name, "record" | "array" | "map" | "enum" | "fixed") {
            return true;
        }
        if matches!(
            (w_name, r_name),
            ("int", "long" | "float" | "double")
                | ("long", "float" | "double")
                | ("float", "double")
                | ("string", "bytes")
                | ("bytes", "string")
        ) {
            return true;
        }
        if w_name == "array" && r_name == "array" {
            let wi = writer.get("items").unwrap_or(&Value::Null);
            let ri = reader.get("items").unwrap_or(&Value::Null);
            return check_reader_writer_compat(ri, wi, "array").is_ok();
        }
        if w_name == "map" && r_name == "map" {
            let wv = writer.get("values").unwrap_or(&Value::Null);
            let rv = reader.get("values").unwrap_or(&Value::Null);
            return check_reader_writer_compat(rv, wv, "map").is_ok();
        }
    }
    false
}

/// Thread-safe client communicating with Confluent, Apicurio, or AWS Glue Schema
/// Registry, with in-memory ID -> [`AvroSchema`] and schema -> ID caches.
#[derive(Debug)]
pub(crate) struct SchemaRegistryClient {
    config: SchemaRegistryConfig,
    http: reqwest::Client,
    rt: Arc<Mutex<tokio::runtime::Runtime>>,
    id32_cache: RwLock<HashMap<u32, Arc<AvroSchema>>>,
    id64_cache: RwLock<HashMap<u64, Arc<AvroSchema>>>,
    uuid_cache: RwLock<HashMap<Uuid, Arc<AvroSchema>>>,
    write_cache: RwLock<HashMap<String, RegistrySchemaId>>,
}

impl SchemaRegistryClient {
    /// Construct a client from `config`, installing the rustls `ring` provider if
    /// none is active yet.
    pub(crate) fn new(config: SchemaRegistryConfig) -> Result<Self, SaciError> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        if config.endpoint.trim().is_empty() {
            return Err(SaciError::configuration(
                "avro registry: 'registry_url' must not be empty",
            ));
        }
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .map_err(|e| {
                SaciError::generic(format!("avro registry: HTTP client init failed: {e}"))
            })?;
        let rt = Arc::new(Mutex::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| SaciError::generic(format!("avro registry runtime build: {e}")))?,
        ));
        Ok(Self {
            config,
            http,
            rt,
            id32_cache: RwLock::new(HashMap::new()),
            id64_cache: RwLock::new(HashMap::new()),
            uuid_cache: RwLock::new(HashMap::new()),
            write_cache: RwLock::new(HashMap::new()),
        })
    }

    pub(crate) fn config(&self) -> &SchemaRegistryConfig {
        &self.config
    }

    /// Fetch the Avro writer schema identified by a 32-bit registry id
    /// (Confluent `id` or Apicurio 4-byte `globalId`/`contentId`).
    pub(crate) fn fetch_schema_by_id32(&self, id: u32) -> Result<Arc<AvroSchema>, SaciError> {
        if let Ok(guard) = self.id32_cache.read()
            && let Some(cached) = guard.get(&id)
        {
            return Ok(Arc::clone(cached));
        }
        let schema_json = match self.config.kind {
            SchemaRegistryKind::Confluent => {
                let url = format!("{}/schemas/ids/{id}", self.config.endpoint);
                let body = self.send_json(Method::GET, &url, None, &[])?;
                let parsed: Value = serde_json::from_str(&body).map_err(|e| {
                    SaciError::generic(format!("avro registry: invalid response JSON: {e}"))
                })?;
                parsed
                    .get("schema")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        SaciError::generic(format!(
                            "avro registry: Confluent /schemas/ids/{id} missing 'schema' field"
                        ))
                    })?
                    .to_string()
            }
            SchemaRegistryKind::Apicurio => self.fetch_apicurio_raw(u64::from(id))?,
            SchemaRegistryKind::Glue => {
                return Err(SaciError::configuration(
                    "avro registry: AWS Glue uses 18-byte UUID headers, not 32-bit schema IDs",
                ));
            }
        };
        let parsed = Arc::new(AvroSchema::new(schema_json));
        if let Ok(mut guard) = self.id32_cache.write() {
            guard.insert(id, Arc::clone(&parsed));
        }
        Ok(parsed)
    }

    /// Fetch the Avro writer schema identified by a 64-bit Apicurio global/content ID.
    pub(crate) fn fetch_schema_by_id64(&self, id: u64) -> Result<Arc<AvroSchema>, SaciError> {
        if let Ok(guard) = self.id64_cache.read()
            && let Some(cached) = guard.get(&id)
        {
            return Ok(Arc::clone(cached));
        }
        if self.config.kind != SchemaRegistryKind::Apicurio {
            return Err(SaciError::configuration(
                "avro registry: 8-byte schema IDs require registry_kind='apicurio'",
            ));
        }
        let schema_json = self.fetch_apicurio_raw(id)?;
        let parsed = Arc::new(AvroSchema::new(schema_json));
        if let Ok(mut guard) = self.id64_cache.write() {
            guard.insert(id, Arc::clone(&parsed));
        }
        Ok(parsed)
    }

    /// Fetch the Avro writer schema identified by an AWS Glue schema version UUID.
    pub(crate) fn fetch_schema_by_uuid(&self, uuid: Uuid) -> Result<Arc<AvroSchema>, SaciError> {
        if let Ok(guard) = self.uuid_cache.read()
            && let Some(cached) = guard.get(&uuid)
        {
            return Ok(Arc::clone(cached));
        }
        if self.config.kind != SchemaRegistryKind::Glue {
            return Err(SaciError::configuration(
                "avro registry: Glue 18-byte headers require registry_kind='glue'",
            ));
        }
        let payload = json!({ "SchemaVersionId": uuid.to_string() });
        let body = self.send_glue("AWSGlue.GetSchemaVersion", &payload)?;
        let parsed: Value = serde_json::from_str(&body).map_err(|e| {
            SaciError::generic(format!("avro registry: invalid Glue response JSON: {e}"))
        })?;
        let schema_json = parsed
            .get("SchemaDefinition")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                SaciError::generic(format!(
                    "avro registry: Glue GetSchemaVersion({uuid}) missing 'SchemaDefinition'"
                ))
            })?
            .to_string();
        let parsed_schema = Arc::new(AvroSchema::new(schema_json));
        if let Ok(mut guard) = self.uuid_cache.write() {
            guard.insert(uuid, Arc::clone(&parsed_schema));
        }
        Ok(parsed_schema)
    }

    /// Resolve, compatibility-check, and (when `auto_register = true`) register
    /// `schema_json` for writing messages.
    pub(crate) fn resolve_or_register_for_write(
        &self,
        schema_json: &str,
    ) -> Result<RegistrySchemaId, SaciError> {
        if let Ok(guard) = self.write_cache.read()
            && let Some(id) = guard.get(schema_json)
        {
            return Ok(*id);
        }
        let id = match self.config.kind {
            SchemaRegistryKind::Confluent => self.resolve_confluent_write(schema_json)?,
            SchemaRegistryKind::Apicurio => self.resolve_apicurio_write(schema_json)?,
            SchemaRegistryKind::Glue => self.resolve_glue_write(schema_json)?,
        };
        if let Ok(mut guard) = self.write_cache.write() {
            guard.insert(schema_json.to_string(), id);
        }
        Ok(id)
    }

    fn subject_name(&self) -> Result<&str, SaciError> {
        self.config
            .subject
            .as_deref()
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| {
                SaciError::configuration(
                    "avro registry: 'subject' must be set to register or look up schemas on write",
                )
            })
    }

    fn fetch_apicurio_raw(&self, id: u64) -> Result<String, SaciError> {
        let global_url = format!(
            "{}/apis/registry/v2/ids/globalIds/{id}",
            self.config.endpoint
        );
        let body = match self.send_json(Method::GET, &global_url, None, &[]) {
            Ok(b) => b,
            Err(_) => {
                let content_url = format!(
                    "{}/apis/registry/v2/ids/contentIds/{id}",
                    self.config.endpoint
                );
                self.send_json(Method::GET, &content_url, None, &[])?
            }
        };
        Ok(extract_schema_text(&body))
    }

    fn resolve_confluent_write(&self, schema_json: &str) -> Result<RegistrySchemaId, SaciError> {
        let subject = url_encode(self.subject_name()?);
        if self.config.compatibility != CompatibilityMode::None {
            let mut checked_any = false;
            if self.config.compatibility.is_transitive() {
                let versions_url = format!("{}/subjects/{subject}/versions", self.config.endpoint);
                if let Ok((status, body)) = self.send_raw(Method::GET, &versions_url, None, &[])
                    && status == 200
                    && let Ok(versions) = serde_json::from_str::<Vec<u64>>(&body)
                {
                    for v in versions {
                        let ver_url =
                            format!("{}/subjects/{subject}/versions/{v}", self.config.endpoint);
                        if let Ok((st, ver_body)) = self.send_raw(Method::GET, &ver_url, None, &[])
                            && st == 200
                            && let Ok(val) = serde_json::from_str::<Value>(&ver_body)
                            && let Some(existing) = val.get("schema").and_then(Value::as_str)
                        {
                            check_avro_compatibility(
                                schema_json,
                                existing,
                                self.config.compatibility,
                            )?;
                            checked_any = true;
                        }
                    }
                }
            }
            if !checked_any {
                let latest_url = format!(
                    "{}/subjects/{subject}/versions/latest",
                    self.config.endpoint
                );
                if let Ok((status, body)) = self.send_raw(Method::GET, &latest_url, None, &[])
                    && status == 200
                    && let Ok(val) = serde_json::from_str::<Value>(&body)
                    && let Some(existing) = val.get("schema").and_then(Value::as_str)
                {
                    check_avro_compatibility(schema_json, existing, self.config.compatibility)?;
                }
            }
        }

        let payload = serde_json::to_string(&json!({
            "schema": schema_json,
            "schemaType": "AVRO"
        }))
        .map_err(|e| SaciError::generic(format!("avro registry: serialize request: {e}")))?;

        let url = if self.config.auto_register {
            format!("{}/subjects/{subject}/versions", self.config.endpoint)
        } else {
            format!("{}/subjects/{subject}", self.config.endpoint)
        };
        let body = self.send_json(Method::POST, &url, Some(payload), &[])?;
        let val: Value = serde_json::from_str(&body).map_err(|e| {
            SaciError::generic(format!(
                "avro registry: invalid Confluent response JSON: {e}"
            ))
        })?;
        let id = val
            .get("id")
            .and_then(Value::as_u64)
            .and_then(|v| u32::try_from(v).ok())
            .ok_or_else(|| {
                SaciError::generic("avro registry: Confluent response missing integer 'id'")
            })?;
        Ok(RegistrySchemaId::Id32(id))
    }

    fn resolve_apicurio_write(&self, schema_json: &str) -> Result<RegistrySchemaId, SaciError> {
        let group = url_encode(&self.config.apicurio_group);
        let subject = url_encode(self.subject_name()?);

        if self.config.compatibility != CompatibilityMode::None {
            let mut checked_any = false;
            if self.config.compatibility.is_transitive() {
                let versions_url = format!(
                    "{}/apis/registry/v2/groups/{group}/artifacts/{subject}/versions",
                    self.config.endpoint
                );
                if let Ok((status, body)) = self.send_raw(Method::GET, &versions_url, None, &[])
                    && status == 200
                    && let Ok(val) = serde_json::from_str::<Value>(&body)
                {
                    let items = val
                        .get("versions")
                        .and_then(Value::as_array)
                        .or_else(|| val.as_array());
                    if let Some(list) = items {
                        for item in list {
                            let ver = item
                                .get("version")
                                .and_then(Value::as_str)
                                .map(str::to_string)
                                .or_else(|| {
                                    item.get("version")
                                        .and_then(Value::as_u64)
                                        .map(|n| n.to_string())
                                });
                            if let Some(ver) = ver {
                                let ver_url = format!(
                                    "{}/apis/registry/v2/groups/{group}/artifacts/{subject}/versions/{}",
                                    self.config.endpoint,
                                    url_encode(&ver)
                                );
                                if let Ok((st, ver_body)) =
                                    self.send_raw(Method::GET, &ver_url, None, &[])
                                    && st == 200
                                {
                                    let existing = extract_schema_text(&ver_body);
                                    check_avro_compatibility(
                                        schema_json,
                                        &existing,
                                        self.config.compatibility,
                                    )?;
                                    checked_any = true;
                                }
                            }
                        }
                    }
                }
            }
            if !checked_any {
                let latest_url = format!(
                    "{}/apis/registry/v2/groups/{group}/artifacts/{subject}",
                    self.config.endpoint
                );
                if let Ok((status, body)) = self.send_raw(Method::GET, &latest_url, None, &[])
                    && status == 200
                {
                    let existing = extract_schema_text(&body);
                    check_avro_compatibility(schema_json, &existing, self.config.compatibility)?;
                }
            }
        }

        let body = if self.config.auto_register {
            let url = format!(
                "{}/apis/registry/v2/groups/{group}/artifacts?ifExists=RETURN_OR_UPDATE",
                self.config.endpoint
            );
            let headers = [("X-Registry-ArtifactId", self.subject_name()?)];
            self.send_json(Method::POST, &url, Some(schema_json.to_string()), &headers)?
        } else {
            let url = format!(
                "{}/apis/registry/v2/groups/{group}/artifacts/{subject}/meta",
                self.config.endpoint
            );
            self.send_json(Method::POST, &url, Some(schema_json.to_string()), &[])?
        };

        let val: Value = serde_json::from_str(&body).map_err(|e| {
            SaciError::generic(format!(
                "avro registry: invalid Apicurio response JSON: {e}"
            ))
        })?;
        let raw_id = val
            .get("globalId")
            .or_else(|| val.get("contentId"))
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                SaciError::generic(
                    "avro registry: Apicurio response missing 'globalId' or 'contentId'",
                )
            })?;
        if self.config.apicurio_eight_byte_id {
            Ok(RegistrySchemaId::Id64(raw_id))
        } else {
            let id32 = u32::try_from(raw_id).map_err(|_| {
                SaciError::configuration(format!(
                    "avro registry: Apicurio id {raw_id} exceeds u32; enable \
                     apicurio_eight_byte_id=#true"
                ))
            })?;
            Ok(RegistrySchemaId::Id32(id32))
        }
    }

    fn resolve_glue_write(&self, schema_json: &str) -> Result<RegistrySchemaId, SaciError> {
        let schema_name = self
            .config
            .glue_schema_name
            .as_deref()
            .or(self.config.subject.as_deref())
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| {
                SaciError::configuration(
                    "avro registry: 'glue_schema_name' or 'subject' must be set for AWS Glue",
                )
            })?;
        let schema_id_obj = json!({
            "RegistryName": self.config.glue_registry_name,
            "SchemaName": schema_name,
        });

        if self.config.compatibility != CompatibilityMode::None {
            let mut checked_any = false;
            if self.config.compatibility.is_transitive()
                && let Ok(versions) =
                    self.glue_list_schema_versions(&self.config.glue_registry_name, schema_name)
            {
                for existing in &versions {
                    check_avro_compatibility(schema_json, existing, self.config.compatibility)?;
                    checked_any = true;
                }
            }
            if !checked_any {
                let latest_req = json!({
                    "SchemaId": schema_id_obj,
                    "SchemaVersionNumber": { "LatestVersion": true },
                });
                if let Ok(latest_body) = self.send_glue("AWSGlue.GetSchemaVersion", &latest_req)
                    && let Ok(val) = serde_json::from_str::<Value>(&latest_body)
                    && let Some(existing) = val.get("SchemaDefinition").and_then(Value::as_str)
                {
                    check_avro_compatibility(schema_json, existing, self.config.compatibility)?;
                }
            }
        }

        let body = if self.config.auto_register {
            let register_req = json!({
                "SchemaId": schema_id_obj,
                "SchemaDefinition": schema_json,
            });
            match self.send_glue("AWSGlue.RegisterSchemaVersion", &register_req) {
                Ok(b) => b,
                Err(_) => {
                    let create_req = json!({
                        "RegistryId": { "RegistryName": self.config.glue_registry_name },
                        "SchemaName": schema_name,
                        "DataFormat": "AVRO",
                        "Compatibility": self.config.compatibility.glue_str(),
                        "SchemaDefinition": schema_json,
                    });
                    self.send_glue("AWSGlue.CreateSchema", &create_req)?
                }
            }
        } else {
            let lookup_req = json!({
                "SchemaId": schema_id_obj,
                "SchemaDefinition": schema_json,
            });
            self.send_glue("AWSGlue.GetSchemaByDefinition", &lookup_req)?
        };

        let val: Value = serde_json::from_str(&body).map_err(|e| {
            SaciError::generic(format!("avro registry: invalid Glue response JSON: {e}"))
        })?;
        let raw_uuid = val
            .get("SchemaVersionId")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                SaciError::generic("avro registry: Glue response missing 'SchemaVersionId'")
            })?;
        let uuid = Uuid::parse_str(raw_uuid).map_err(|e| {
            SaciError::generic(format!(
                "avro registry: invalid Glue UUID '{raw_uuid}': {e}"
            ))
        })?;
        Ok(RegistrySchemaId::GlueUuid(uuid))
    }

    fn glue_list_schema_versions(
        &self,
        registry_name: &str,
        schema_name: &str,
    ) -> Result<Vec<String>, SaciError> {
        let schema_id_obj = json!({
            "RegistryName": registry_name,
            "SchemaName": schema_name,
        });
        let list_req = json!({
            "SchemaId": schema_id_obj,
            "MaxResults": 100,
        });
        let list_body = self.send_glue("AWSGlue.ListSchemaVersions", &list_req)?;
        let list_val: Value = serde_json::from_str(&list_body).map_err(|e| {
            SaciError::generic(format!("avro registry: invalid Glue list response: {e}"))
        })?;
        let Some(entries) = list_val.get("Schemas").and_then(Value::as_array) else {
            return Ok(Vec::new());
        };
        let mut versions = Vec::new();
        for entry in entries {
            let available = entry
                .get("Status")
                .and_then(Value::as_str)
                .is_none_or(|s| s == "AVAILABLE");
            if !available {
                continue;
            }
            if let Some(ver_num) = entry.get("VersionNumber").and_then(Value::as_u64) {
                let get_req = json!({
                    "SchemaId": schema_id_obj,
                    "SchemaVersionNumber": { "VersionNumber": ver_num },
                });
                if let Ok(ver_body) = self.send_glue("AWSGlue.GetSchemaVersion", &get_req)
                    && let Ok(ver_val) = serde_json::from_str::<Value>(&ver_body)
                    && let Some(def) = ver_val.get("SchemaDefinition").and_then(Value::as_str)
                {
                    versions.push(def.to_string());
                }
            }
        }
        Ok(versions)
    }

    fn send_glue(&self, target: &str, payload: &Value) -> Result<String, SaciError> {
        let body = serde_json::to_string(payload)
            .map_err(|e| SaciError::generic(format!("avro registry: serialize Glue body: {e}")))?;
        let url = format!("{}/", self.config.endpoint);
        let extra = [
            ("X-Amz-Target", target),
            ("Content-Type", "application/x-amz-json-1.1"),
        ];
        self.send_json(Method::POST, &url, Some(body), &extra)
    }

    fn send_json(
        &self,
        method: Method,
        url: &str,
        body: Option<String>,
        extra_headers: &[(&str, &str)],
    ) -> Result<String, SaciError> {
        let (status, text) = self.send_raw(method, url, body, extra_headers)?;
        if !(200..300).contains(&status) {
            return Err(SaciError::generic(format!(
                "avro registry HTTP {status} from {url}: {text}"
            )));
        }
        Ok(text)
    }

    fn send_raw(
        &self,
        method: Method,
        url: &str,
        body: Option<String>,
        extra_headers: &[(&str, &str)],
    ) -> Result<(u16, String), SaciError> {
        let http = self.http.clone();
        let rt = Arc::clone(&self.rt);
        let auth = self.config.auth.clone();
        let url_owned = url.to_string();
        let headers_owned: Vec<(String, String)> = extra_headers
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();

        // Serialize `block_on` across threads because `current_thread` Tokio runtimes
        // panic if entered concurrently from multiple OS threads.
        std::thread::scope(|scope| {
            scope
                .spawn(move || {
                    let rt_guard = rt.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                    rt_guard.block_on(async move {
                        let mut req = http.request(method.clone(), &url_owned);
                        let has_content_type = headers_owned
                            .iter()
                            .any(|(k, _)| k.eq_ignore_ascii_case("content-type"));
                        if body.is_some() && !has_content_type {
                            req = req.header("Content-Type", "application/json");
                        }
                        for (k, v) in &headers_owned {
                            req = req.header(k.as_str(), v.as_str());
                        }
                        match &auth {
                            RegistryAuth::None => {}
                            RegistryAuth::Basic { username, password } => {
                                req = req.basic_auth(username, Some(password));
                            }
                            RegistryAuth::Bearer { token } => {
                                req = req.bearer_auth(token);
                            }
                            RegistryAuth::AwsSigV4 {
                                access_key_id,
                                secret_access_key,
                                session_token,
                                region,
                            } => {
                                let payload = body.as_deref().unwrap_or("");
                                let sig_headers = sign_aws_sigv4(
                                    method.as_str(),
                                    &url_owned,
                                    region,
                                    "glue",
                                    payload,
                                    &headers_owned,
                                    access_key_id.as_deref(),
                                    secret_access_key.as_deref(),
                                    session_token.as_deref(),
                                )?;
                                for (k, v) in sig_headers {
                                    req = req.header(k, v);
                                }
                            }
                        }
                        if let Some(b) = body {
                            req = req.body(b);
                        }
                        let resp = req.send().await.map_err(|e| {
                            SaciError::generic(format!("avro registry HTTP error: {e}"))
                        })?;
                        let status = resp.status().as_u16();
                        let text = resp.text().await.map_err(|e| {
                            SaciError::generic(format!("avro registry body error: {e}"))
                        })?;
                        Ok((status, text))
                    })
                })
                .join()
                .map_err(|_| SaciError::generic("avro registry thread panicked"))?
        })
    }
}

fn extract_schema_text(raw: &str) -> String {
    if let Ok(val) = serde_json::from_str::<Value>(raw)
        && let Some(s) = val.get("schema").and_then(Value::as_str)
    {
        return s.to_string();
    }
    raw.to_string()
}

fn url_encode(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            _ => {
                out.push('%');
                out.push_str(&format!("{byte:02X}"));
            }
        }
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn sign_aws_sigv4(
    method: &str,
    url: &str,
    region: &str,
    service: &str,
    payload: &str,
    extra_headers: &[(String, String)],
    access_key_opt: Option<&str>,
    secret_key_opt: Option<&str>,
    session_token_opt: Option<&str>,
) -> Result<Vec<(String, String)>, SaciError> {
    let access_key = access_key_opt
        .map(str::to_string)
        .or_else(|| std::env::var("AWS_ACCESS_KEY_ID").ok())
        .ok_or_else(|| {
            SaciError::configuration(
                "avro registry: AWS SigV4 access key not set (set credentials or AWS_ACCESS_KEY_ID)",
            )
        })?;
    let secret_key = secret_key_opt
        .map(str::to_string)
        .or_else(|| std::env::var("AWS_SECRET_ACCESS_KEY").ok())
        .ok_or_else(|| {
            SaciError::configuration(
                "avro registry: AWS SigV4 secret key not set (set credentials or AWS_SECRET_ACCESS_KEY)",
            )
        })?;
    let session_token = session_token_opt
        .map(str::to_string)
        .or_else(|| std::env::var("AWS_SESSION_TOKEN").ok());

    let parsed = reqwest::Url::parse(url).map_err(|e| {
        SaciError::configuration(format!("avro registry: invalid URL '{url}': {e}"))
    })?;
    let host = parsed
        .host_str()
        .map(|h| match parsed.port() {
            Some(p) => format!("{h}:{p}"),
            None => h.to_string(),
        })
        .unwrap_or_default();
    let canonical_uri = parsed.path();
    let canonical_query = parsed.query().unwrap_or("");

    let now = chrono::Utc::now();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date_stamp = now.format("%Y%m%d").to_string();

    let payload_hash = hex::encode(digest::digest(&digest::SHA256, payload.as_bytes()));
    let mut canonical_map: BTreeMap<String, String> = BTreeMap::new();
    canonical_map.insert("host".to_string(), host.clone());
    canonical_map.insert("x-amz-content-sha256".to_string(), payload_hash.clone());
    canonical_map.insert("x-amz-date".to_string(), amz_date.clone());
    if let Some(token) = &session_token {
        canonical_map.insert("x-amz-security-token".to_string(), token.trim().to_string());
    }
    for (k, v) in extra_headers {
        let lower = k.to_ascii_lowercase();
        if lower.starts_with("x-amz-") {
            canonical_map.insert(lower, v.trim().to_string());
        }
    }

    let mut canonical_headers = String::new();
    let mut signed_keys = Vec::with_capacity(canonical_map.len());
    for (k, v) in &canonical_map {
        canonical_headers.push_str(k);
        canonical_headers.push(':');
        canonical_headers.push_str(v);
        canonical_headers.push('\n');
        signed_keys.push(k.as_str());
    }
    let signed_headers = signed_keys.join(";");
    let canonical_request = format!(
        "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    );
    let canonical_hash = hex::encode(digest::digest(
        &digest::SHA256,
        canonical_request.as_bytes(),
    ));

    let credential_scope = format!("{date_stamp}/{region}/{service}/aws4_request");
    let string_to_sign =
        format!("AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{canonical_hash}");

    let k_secret = format!("AWS4{secret_key}");
    let k_date = hmac_sha256(k_secret.as_bytes(), date_stamp.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature = hex::encode(hmac_sha256(&k_signing, string_to_sign.as_bytes()));

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, \
         SignedHeaders={signed_headers}, Signature={signature}"
    );

    let mut out = vec![
        ("Host".to_string(), host),
        ("X-Amz-Date".to_string(), amz_date),
        ("X-Amz-Content-Sha256".to_string(), payload_hash),
        ("Authorization".to_string(), authorization),
    ];
    if let Some(token) = session_token {
        out.push(("X-Amz-Security-Token".to_string(), token));
    }
    Ok(out)
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let signing_key = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::sign(&signing_key, data).as_ref().to_vec()
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn orders_v1_json() -> &'static str {
        r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"int"},{"name":"amount","type":"float"}]}"#
    }

    fn orders_v2_backward_json() -> &'static str {
        // Widens id: int -> long and amount: float -> double, plus adds nullable/default note
        r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"long"},{"name":"amount","type":"double"},{"name":"note","type":["null","string"],"default":null}]}"#
    }

    fn orders_v2_breaking_json() -> &'static str {
        // Adds required field without default -> incompatible in backward mode
        r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"long"},{"name":"required_tag","type":"string"}]}"#
    }

    #[test]
    fn compatibility_checks_backward_forward_and_full_rules() {
        check_avro_compatibility(
            orders_v2_backward_json(),
            orders_v1_json(),
            CompatibilityMode::Backward,
        )
        .expect("widening and adding defaulted field is backward compatible");

        let err = check_avro_compatibility(
            orders_v2_breaking_json(),
            orders_v1_json(),
            CompatibilityMode::Backward,
        )
        .expect_err("required field without default breaks backward compatibility");
        assert!(err.message().contains("required_tag"), "got: {err}");

        // Existing schema v1 (with note not present) reading v2: adding defaulted field with default in v2 is forward compatible if v1 doesn't narrow id/amount.
        let v1 = r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"long"}]}"#;
        let v2_with_default = r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"long"},{"name":"extra","type":"string","default":"x"}]}"#;
        check_avro_compatibility(v2_with_default, v1, CompatibilityMode::Full)
            .expect("full compatibility passes when new field has default and existing fields stay compatible");
    }

    fn spawn_mock_server(
        handler: impl Fn(&str, &str, &str) -> (u16, String) + Send + Sync + 'static,
    ) -> (String, std::sync::Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock registry");
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_bg = Arc::clone(&hits);
        let handler = Arc::new(handler);

        std::thread::spawn(move || {
            for stream in listener.incoming().take(12) {
                let Ok(mut stream) = stream else {
                    break;
                };
                hits_bg.fetch_add(1, Ordering::SeqCst);
                let mut buf = [0u8; 8192];
                let n = stream.read(&mut buf).unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                let mut lines = req.lines();
                let req_line = lines.next().unwrap_or("");
                let mut parts = req_line.split_whitespace();
                let method = parts.next().unwrap_or("GET");
                let path = parts.next().unwrap_or("/");
                let body = req.split("\r\n\r\n").nth(1).unwrap_or("");
                let (status, resp_body) = handler(method, path, body);
                let reply = format!(
                    "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{resp_body}",
                    resp_body.len()
                );
                let _ = stream.write_all(reply.as_bytes());
                let _ = stream.flush();
            }
        });
        (format!("http://{addr}"), hits)
    }

    #[test]
    fn confluent_client_registers_fetches_and_caches_schemas() {
        let v2_json = serde_json::to_string(orders_v2_backward_json()).unwrap();
        let v1_json = serde_json::to_string(orders_v1_json()).unwrap();
        let (endpoint, hits) = spawn_mock_server(move |method, path, _body| match (method, path) {
            ("GET", "/subjects/orders-value/versions/latest") => {
                (200, format!(r#"{{"id":1,"schema":{v1_json}}}"#))
            }
            ("POST", "/subjects/orders-value/versions") => (200, r#"{"id":17}"#.to_string()),
            ("GET", "/schemas/ids/17") => (200, format!(r#"{{"schema":{v2_json}}}"#)),
            _ => (404, r#"{"error_code":40403}"#.to_string()),
        });

        let cfg = SchemaRegistryConfig::confluent(endpoint)
            .with_subject("orders-value")
            .with_compatibility(CompatibilityMode::Backward);
        let client = SchemaRegistryClient::new(cfg).expect("client");

        let reg_id = client
            .resolve_or_register_for_write(orders_v2_backward_json())
            .expect("register v2");
        assert_eq!(reg_id, RegistrySchemaId::Id32(17));

        let fetched_1 = client.fetch_schema_by_id32(17).expect("fetch id 17");
        let hits_after_first = hits.load(Ordering::SeqCst);
        let fetched_2 = client.fetch_schema_by_id32(17).expect("cached id 17");
        assert_eq!(
            hits.load(Ordering::SeqCst),
            hits_after_first,
            "second lookup must be served from cache"
        );
        assert_eq!(fetched_1.json_string, fetched_2.json_string);
    }

    #[test]
    fn apicurio_eight_byte_and_glue_sigv4_clients_roundtrip() {
        let schema_str = orders_v1_json().to_string();
        let (apicurio_url, _) = spawn_mock_server(move |method, path, _body| {
            if method == "POST" && path.contains("/artifacts") {
                (200, r#"{"globalId":9001}"#.to_string())
            } else if method == "GET" && path == "/apis/registry/v2/ids/globalIds/9001" {
                (200, schema_str.clone())
            } else {
                (404, "{}".to_string())
            }
        });
        let apicurio_cfg = SchemaRegistryConfig::apicurio(apicurio_url)
            .with_subject("orders")
            .with_apicurio_eight_byte_id(true);
        let apicurio_client = SchemaRegistryClient::new(apicurio_cfg).expect("client");
        assert_eq!(
            apicurio_client
                .resolve_or_register_for_write(orders_v1_json())
                .unwrap(),
            RegistrySchemaId::Id64(9001)
        );
        assert!(
            apicurio_client
                .fetch_schema_by_id64(9001)
                .unwrap()
                .json_string
                .contains("\"Order\"")
        );

        let glue_uuid = Uuid::new_v4();
        let glue_uuid_str = glue_uuid.to_string();
        let glue_schema = orders_v1_json().to_string();
        let (glue_url, _) = spawn_mock_server(move |method, _path, body| {
            assert_eq!(method, "POST");
            if body.contains("SchemaDefinition") {
                (200, format!(r#"{{"SchemaVersionId":"{glue_uuid_str}"}}"#))
            } else {
                (200, json!({ "SchemaDefinition": glue_schema }).to_string())
            }
        });
        let glue_cfg = SchemaRegistryConfig::glue(glue_url, "us-east-1")
            .with_glue_names("reg", "orders")
            .with_auth(RegistryAuth::AwsSigV4 {
                access_key_id: Some("AKIAEXAMPLE".to_string()),
                secret_access_key: Some("secret".to_string()),
                session_token: None,
                region: "us-east-1".to_string(),
            });
        let glue_client = SchemaRegistryClient::new(glue_cfg).expect("glue client");
        let id = glue_client
            .resolve_or_register_for_write(orders_v1_json())
            .expect("glue register");
        assert_eq!(id, RegistrySchemaId::GlueUuid(glue_uuid));
        assert!(
            glue_client
                .fetch_schema_by_uuid(glue_uuid)
                .unwrap()
                .json_string
                .contains("\"Order\"")
        );
    }

    #[test]
    fn union_to_union_and_transitive_compatibility_checks() {
        let union_int = r#"{"type":"record","name":"U","fields":[{"name":"x","type":["null","int"],"default":null}]}"#;
        let union_long = r#"{"type":"record","name":"U","fields":[{"name":"x","type":["null","long"],"default":null}]}"#;
        check_avro_compatibility(union_long, union_int, CompatibilityMode::Backward)
            .expect("union widening int->long must be backward compatible");

        // v1 has int `id`; v2 widens `id` to long and adds defaulted `note`; v3 drops `id` and widens?
        // Actually v1 requires `id: int` and `tag: string` (default `"a"`).
        // v2 keeps `id: long` (backward compatible with v1) and adds `v2_req` with default.
        // v3 removes `id` (or narrows `id` back to int?Wait: in Backward, v3 reader reads v1 writer):
        // Better: v1 has `extra_req` with NO default in v1? No: v3 adds `strict_since_v2` which was present in v2 WITHOUT default, absent in v1!
        let v1 = r#"{"type":"record","name":"Ev","fields":[{"name":"id","type":"int"}]}"#;
        let v2 = r#"{"type":"record","name":"Ev","fields":[{"name":"id","type":"int"},{"name":"mid","type":"string","default":"ok"}]}"#;
        let v3 = r#"{"type":"record","name":"Ev","fields":[{"name":"id","type":"int"},{"name":"mid","type":"string"}]}"#;
        // v3 removed default from `mid`: backward-compatible with v2 (which has `mid`), but breaks v1 (which lacks `mid`).
        check_avro_compatibility(v3, v2, CompatibilityMode::Backward)
            .expect("v3 vs v2 alone is backward compatible");
        assert!(
            check_avro_compatibility(v3, v1, CompatibilityMode::BackwardTransitive).is_err(),
            "v3 vs v1 must fail backward compatibility"
        );

        let v1_s = serde_json::to_string(v1).unwrap();
        let v2_s = serde_json::to_string(v2).unwrap();
        let (endpoint, _) = spawn_mock_server(move |method, path, _body| match (method, path) {
            ("GET", "/subjects/ev-value/versions") => (200, "[1,2]".to_string()),
            ("GET", "/subjects/ev-value/versions/1") => {
                (200, format!(r#"{{"id":1,"schema":{v1_s}}}"#))
            }
            ("GET", "/subjects/ev-value/versions/2") => {
                (200, format!(r#"{{"id":2,"schema":{v2_s}}}"#))
            }
            ("GET", "/subjects/ev-value/versions/latest") => {
                (200, format!(r#"{{"id":2,"schema":{v2_s}}}"#))
            }
            ("POST", "/subjects/ev-value/versions") => (200, r#"{"id":3}"#.to_string()),
            _ => (404, "{}".to_string()),
        });

        let trans_cfg = SchemaRegistryConfig::confluent(&endpoint)
            .with_subject("ev-value")
            .with_compatibility(CompatibilityMode::BackwardTransitive);
        let trans_client = SchemaRegistryClient::new(trans_cfg).unwrap();
        let err = trans_client
            .resolve_or_register_for_write(v3)
            .expect_err("BackwardTransitive must reject schema incompatible with historical v1");
        assert!(err.message().contains("mid"), "got: {err}");
    }

    #[test]
    fn glue_sigv4_signs_session_token_and_target_and_maps_glue_compatibility() {
        assert_eq!(
            CompatibilityMode::BackwardTransitive.glue_str(),
            "BACKWARD_ALL"
        );
        assert_eq!(
            CompatibilityMode::ForwardTransitive.glue_str(),
            "FORWARD_ALL"
        );
        assert_eq!(CompatibilityMode::FullTransitive.glue_str(), "FULL_ALL");

        let headers = sign_aws_sigv4(
            "POST",
            "https://glue.us-east-1.amazonaws.com/",
            "us-east-1",
            "glue",
            "{}",
            &[(
                "X-Amz-Target".to_string(),
                "AWSGlue.CreateSchema".to_string(),
            )],
            Some("AKIAEXAMPLE"),
            Some("secret"),
            Some("STS_TOKEN"),
        )
        .expect("sigv4 succeeds");
        let auth = headers
            .iter()
            .find(|(k, _)| k == "Authorization")
            .map(|(_, v)| v.as_str())
            .unwrap();
        assert!(
            auth.contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token;x-amz-target"),
            "signed headers missing security token or target: {auth}"
        );
        assert!(
            headers
                .iter()
                .any(|(k, v)| k == "X-Amz-Security-Token" && v == "STS_TOKEN")
        );
    }

    #[test]
    fn concurrent_schema_registry_requests_do_not_panic() {
        let s = serde_json::to_string(orders_v1_json()).unwrap();
        let (endpoint, hits) = spawn_mock_server(move |method, path, _body| match (method, path) {
            ("GET", p) if p.starts_with("/schemas/ids/") => (200, format!(r#"{{"schema":{s}}}"#)),
            _ => (404, "{}".to_string()),
        });
        let cfg = SchemaRegistryConfig::confluent(&endpoint);
        let client = Arc::new(SchemaRegistryClient::new(cfg).expect("client"));
        let mut handles = Vec::new();
        for id in 1u32..=8 {
            let c = Arc::clone(&client);
            handles.push(std::thread::spawn(move || c.fetch_schema_by_id32(id)));
        }
        for handle in handles {
            let res = handle
                .join()
                .expect("thread must not panic on Tokio runtime");
            assert!(res.is_ok(), "concurrent fetch failed: {res:?}");
        }
        assert_eq!(hits.load(Ordering::SeqCst), 8);
    }

    #[test]
    fn glue_transitive_compatibility_checks_all_versions() {
        let v1 = r#"{"type":"record","name":"Ev","fields":[{"name":"id","type":"int"}]}"#;
        let v2 = r#"{"type":"record","name":"Ev","fields":[{"name":"id","type":"int"},{"name":"mid","type":"string","default":"ok"}]}"#;
        let v3 = r#"{"type":"record","name":"Ev","fields":[{"name":"id","type":"int"},{"name":"mid","type":"string"}]}"#;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind glue mock");
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for stream in listener.incoming().take(6) {
                let Ok(mut stream) = stream else { break };
                let mut buf = [0u8; 8192];
                let n = stream.read(&mut buf).unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                let body = req.split("\r\n\r\n").nth(1).unwrap_or("");
                let is_list =
                    req.contains("ListSchemaVersions") || req.contains("listschemaversions");
                let resp_body = if is_list {
                    r#"{"Schemas":[{"VersionNumber":1,"Status":"AVAILABLE"},{"VersionNumber":2,"Status":"AVAILABLE"}]}"#.to_string()
                } else if body.contains("\"VersionNumber\":1") {
                    serde_json::json!({ "SchemaDefinition": v1 }).to_string()
                } else {
                    serde_json::json!({ "SchemaDefinition": v2 }).to_string()
                };
                let reply = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{resp_body}",
                    resp_body.len()
                );
                let _ = stream.write_all(reply.as_bytes());
                let _ = stream.flush();
            }
        });

        let endpoint = format!("http://{addr}");
        let cfg = SchemaRegistryConfig::glue(&endpoint, "us-east-1")
            .with_subject("ev-value")
            .with_compatibility(CompatibilityMode::BackwardTransitive)
            .with_auth(RegistryAuth::AwsSigV4 {
                access_key_id: Some("AKIAEXAMPLE".to_string()),
                secret_access_key: Some("secret".to_string()),
                session_token: None,
                region: "us-east-1".to_string(),
            });
        let client = SchemaRegistryClient::new(cfg).expect("glue client");
        let err = client
            .resolve_or_register_for_write(v3)
            .expect_err("Glue BackwardTransitive must reject v3 incompatible with v1");
        assert!(err.message().contains("mid"), "got: {err}");
    }
}
