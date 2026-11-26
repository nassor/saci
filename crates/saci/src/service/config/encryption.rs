//! Declarative service, codec, and node encryption configuration.
//!
//! Parsed unconditionally in every build (even when the `crypto` feature is
//! disabled) so [`validate_build_capabilities`](crate::service::validation::validate_build_capabilities)
//! can emit a targeted `--features crypto` error rather than an opaque unknown-key
//! parse failure.

use std::collections::HashSet;
use std::path::PathBuf;
#[cfg(feature = "crypto")]
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::config::{ConfigValue, one_or_many};
use crate::error::{SaciError, SaciResult};

fn default_true() -> bool {
    true
}

/// One named 256-bit key source declared inside a top-level `encryption` block.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct KeySpec {
    /// Key identifier (`1..=64` UTF-8 bytes) written into wire envelopes.
    pub id: String,
    /// Name of an environment variable containing hex or base64 key material.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env: Option<String>,
    /// Path to a file containing raw 32-byte, hex, or base64 key material.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<PathBuf>,
    /// Inline 64-character hex key (typically populated via `${VAR}` substitution).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hex: Option<String>,
    /// Inline base64-encoded 32-byte key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base64: Option<String>,
}

impl KeySpec {
    fn validate(&self) -> SaciResult<()> {
        if self.id.is_empty() || self.id.len() > 64 {
            return Err(SaciError::configuration(format!(
                "encryption.key '{}': id must be 1..=64 UTF-8 bytes",
                self.id
            )));
        }
        let sources = usize::from(self.env.is_some())
            + usize::from(self.file.is_some())
            + usize::from(self.hex.is_some())
            + usize::from(self.base64.is_some());
        if sources != 1 {
            return Err(SaciError::configuration(format!(
                "encryption.key '{}': specify exactly one of 'env', 'file', 'hex', or 'base64'",
                self.id
            )));
        }
        if let Some(var) = &self.env
            && var.trim().is_empty()
        {
            return Err(SaciError::configuration(format!(
                "encryption.key '{}': env variable name must not be empty",
                self.id
            )));
        }
        Ok(())
    }

    #[cfg(feature = "crypto")]
    fn load_material(&self) -> SaciResult<crate::crypto::KeyMaterial> {
        use crate::crypto::KeyMaterial;

        if let Some(var) = &self.env {
            let val = std::env::var(var).map_err(|e| {
                SaciError::configuration(format!(
                    "encryption.key '{}': cannot read env var '{var}': {e}",
                    self.id
                ))
            })?;
            let trimmed = val.trim();
            return if trimmed.len() == 64 && trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
                KeyMaterial::from_hex(trimmed)
            } else {
                KeyMaterial::from_base64(trimmed)
            };
        }
        if let Some(path) = &self.file {
            return KeyMaterial::from_file(path);
        }
        if let Some(hex) = &self.hex {
            return KeyMaterial::from_hex(hex);
        }
        if let Some(b64) = &self.base64 {
            return KeyMaterial::from_base64(b64);
        }
        Err(SaciError::configuration(format!(
            "encryption.key '{}': no key source specified",
            self.id
        )))
    }
}

/// Top-level `encryption { ... }` keyring and default AEAD algorithm block.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ServiceEncryptionConfig {
    /// Default AEAD algorithm: `"aes-256-gcm"` (default) or `"chacha20-poly1305"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub algorithm: Option<String>,
    /// Key id used to encrypt new payloads and fields.
    pub active_key: String,
    /// Key entries available for encryption and historical decryption.
    #[serde(
        rename = "key",
        default,
        deserialize_with = "one_or_many",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub keys: Vec<KeySpec>,
}

impl ServiceEncryptionConfig {
    /// Validate keyring structure, algorithm spelling, and `active_key` presence.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] on the first invalid key or setting.
    pub fn validate(&self) -> SaciResult<()> {
        if let Some(alg) = &self.algorithm {
            validate_algorithm_name("encryption", alg)?;
        }
        if self.keys.is_empty() {
            return Err(SaciError::configuration(
                "encryption: at least one 'key' entry is required",
            ));
        }
        let mut ids = HashSet::with_capacity(self.keys.len());
        for key in &self.keys {
            key.validate()?;
            if !ids.insert(key.id.as_str()) {
                return Err(SaciError::configuration(format!(
                    "encryption: duplicate key id '{}'",
                    key.id
                )));
            }
        }
        if !ids.contains(self.active_key.as_str()) {
            return Err(SaciError::configuration(format!(
                "encryption: active_key '{}' is not declared in 'key' entries",
                self.active_key
            )));
        }
        Ok(())
    }

    /// Build a [`StaticKeyProvider`](crate::crypto::StaticKeyProvider) from this block.
    #[cfg(feature = "crypto")]
    pub fn build_key_provider(&self) -> SaciResult<Arc<dyn crate::crypto::KeyProvider>> {
        use std::collections::HashMap;

        use crate::crypto::{KeyMaterial, StaticKeyProvider};

        self.validate()?;
        let mut map: HashMap<String, KeyMaterial> = HashMap::with_capacity(self.keys.len());
        for key in &self.keys {
            map.insert(key.id.clone(), key.load_material()?);
        }
        let provider = StaticKeyProvider::new(self.active_key.clone(), map)?;
        Ok(Arc::new(provider))
    }
}

/// Per-`codec` encryption settings (`codec "..." { encryption ... }`).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CodecEncryptionConfig {
    /// Enable whole-payload encryption/decryption on this codec (default `true`).
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Optional AEAD algorithm override (`"aes-256-gcm"` or `"chacha20-poly1305"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub algorithm: Option<String>,
    /// Optional key id override from the service keyring.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_key: Option<String>,
}

impl CodecEncryptionConfig {
    /// Validate algorithm and key id overrides.
    pub fn validate(&self, context: &str) -> SaciResult<()> {
        if let Some(alg) = &self.algorithm {
            validate_algorithm_name(context, alg)?;
        }
        if let Some(key) = &self.active_key
            && (key.is_empty() || key.len() > 64)
        {
            return Err(SaciError::configuration(format!(
                "{context}: active_key '{key}' must be 1..=64 UTF-8 bytes"
            )));
        }
        Ok(())
    }
}

/// Per-node `encrypt` (on `sink`) or `decrypt` (on `source`) configuration.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct NodeCryptoConfig {
    /// `"fields"` (default when `fields` is set) or `"payload"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    /// Field names to encrypt/decrypt (comma-separated string or KDL string list).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fields: Option<ConfigValue>,
    /// Wire encoding: `"binary"` (default) or `"base64"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encoding: Option<String>,
    /// Optional AEAD algorithm override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub algorithm: Option<String>,
    /// Optional active key id override (sink `encrypt` only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_key: Option<String>,
}

impl NodeCryptoConfig {
    /// Resolve mode as `"fields"` or `"payload"`.
    pub fn resolved_mode(&self, context: &str) -> SaciResult<&'static str> {
        match self.mode.as_deref() {
            None => {
                if self.fields.is_some() {
                    Ok("fields")
                } else {
                    Err(SaciError::configuration(format!(
                        "{context}: specify 'fields' or mode=\"payload\""
                    )))
                }
            }
            Some("fields") => Ok("fields"),
            Some("payload") => Ok("payload"),
            Some(other) => Err(SaciError::configuration(format!(
                "{context}: unknown mode '{other}'; expected \"fields\" or \"payload\""
            ))),
        }
    }

    /// Resolve field names when in `"fields"` mode.
    pub fn resolved_fields(&self, context: &str) -> SaciResult<Vec<String>> {
        let Some(val) = &self.fields else {
            return Err(SaciError::configuration(format!(
                "{context}: mode=\"fields\" requires 'fields'"
            )));
        };
        let mut out = Vec::new();
        match val {
            ConfigValue::String(s) => {
                for part in s.split(',') {
                    let trimmed = part.trim();
                    if !trimmed.is_empty() {
                        out.push(trimmed.to_string());
                    }
                }
            }
            ConfigValue::Array(items) => {
                for item in items {
                    let s = item.as_str().ok_or_else(|| {
                        SaciError::configuration(format!(
                            "{context}: 'fields' entries must be strings"
                        ))
                    })?;
                    let trimmed = s.trim();
                    if !trimmed.is_empty() {
                        out.push(trimmed.to_string());
                    }
                }
            }
            _ => {
                return Err(SaciError::configuration(format!(
                    "{context}: 'fields' must be a string or list of strings"
                )));
            }
        }
        if out.is_empty() {
            return Err(SaciError::configuration(format!(
                "{context}: 'fields' must list at least one field name"
            )));
        }
        let mut seen = HashSet::with_capacity(out.len());
        for name in &out {
            if !seen.insert(name.as_str()) {
                return Err(SaciError::configuration(format!(
                    "{context}: duplicate field '{name}'"
                )));
            }
        }
        Ok(out)
    }

    /// Resolve wire encoding (`"binary"` or `"base64"`).
    pub fn resolved_encoding(&self, context: &str) -> SaciResult<&'static str> {
        match self.encoding.as_deref() {
            None | Some("binary") => Ok("binary"),
            Some("base64") => Ok("base64"),
            Some(other) => Err(SaciError::configuration(format!(
                "{context}: unknown encoding '{other}'; expected \"binary\" or \"base64\""
            ))),
        }
    }

    /// Validate all fields of this node encryption/decryption block.
    pub fn validate(&self, context: &str, allow_active_key: bool) -> SaciResult<()> {
        let mode = self.resolved_mode(context)?;
        if mode == "fields" {
            self.resolved_fields(context)?;
        } else if self.fields.is_some() {
            return Err(SaciError::configuration(format!(
                "{context}: 'fields' cannot be set when mode=\"payload\""
            )));
        }
        self.resolved_encoding(context)?;
        if let Some(alg) = &self.algorithm {
            validate_algorithm_name(context, alg)?;
        }
        if let Some(key) = &self.active_key {
            if !allow_active_key {
                return Err(SaciError::configuration(format!(
                    "{context}: 'active_key' is not allowed on source decrypt (decryption reads key_id from each envelope)"
                )));
            }
            if key.is_empty() || key.len() > 64 {
                return Err(SaciError::configuration(format!(
                    "{context}: active_key '{key}' must be 1..=64 UTF-8 bytes"
                )));
            }
        }
        Ok(())
    }

    /// Convert into [`crate::crypto::EncryptionMode`].
    #[cfg(feature = "crypto")]
    pub fn to_encryption_mode(&self, context: &str) -> SaciResult<crate::crypto::EncryptionMode> {
        use crate::crypto::{EncryptionMode, FieldEncoding};

        let encoding = match self.resolved_encoding(context)? {
            "binary" => FieldEncoding::Binary,
            "base64" => FieldEncoding::Base64Utf8,
            _ => unreachable!(),
        };
        match self.resolved_mode(context)? {
            "fields" => Ok(EncryptionMode::fields_with_encoding(
                self.resolved_fields(context)?,
                encoding,
            )),
            "payload" => Ok(EncryptionMode::payload_with_encoding(encoding)),
            _ => unreachable!(),
        }
    }
}

fn validate_algorithm_name(context: &str, alg: &str) -> SaciResult<()> {
    match alg.trim().to_ascii_lowercase().as_str() {
        "aes-256-gcm" | "aes256gcm" | "chacha20-poly1305" | "chacha20poly1305" => Ok(()),
        other => Err(SaciError::configuration(format!(
            "{context}: unknown algorithm '{other}'; expected 'aes-256-gcm' or 'chacha20-poly1305'"
        ))),
    }
}

#[cfg(feature = "crypto")]
struct ActiveKeyOverride {
    inner: Arc<dyn crate::crypto::KeyProvider>,
    active_key: String,
}

#[cfg(feature = "crypto")]
impl crate::crypto::KeyProvider for ActiveKeyOverride {
    fn active_key(&self) -> Result<crate::crypto::ActiveKey, SaciError> {
        let material = self.inner.key(&self.active_key)?;
        Ok(crate::crypto::ActiveKey {
            id: self.active_key.clone(),
            material,
        })
    }

    fn key(&self, key_id: &str) -> Result<crate::crypto::KeyMaterial, SaciError> {
        self.inner.key(key_id)
    }
}

/// Resolve a [`Cipher`](crate::crypto::Cipher) from injected or config-built provider + overrides.
#[cfg(feature = "crypto")]
pub(crate) fn resolve_cipher(
    context: &str,
    injected: Option<&Arc<dyn crate::crypto::KeyProvider>>,
    service_enc: Option<&ServiceEncryptionConfig>,
    cached_provider: &mut Option<Arc<dyn crate::crypto::KeyProvider>>,
    alg_override: Option<&str>,
    active_key_override: Option<&str>,
) -> SaciResult<Arc<crate::crypto::Cipher>> {
    use crate::crypto::{Algorithm, Cipher};

    let base_provider: Arc<dyn crate::crypto::KeyProvider> = if let Some(p) = injected {
        Arc::clone(p)
    } else if let Some(provider) = cached_provider {
        Arc::clone(provider)
    } else if let Some(cfg) = service_enc {
        let built = cfg.build_key_provider()?;
        *cached_provider = Some(Arc::clone(&built));
        built
    } else {
        return Err(SaciError::configuration(format!(
            "{context}: encryption is configured, but neither a top-level `encryption` block \
             nor `ServiceBuilder::key_provider` was supplied"
        )));
    };

    let provider: Arc<dyn crate::crypto::KeyProvider> = match active_key_override {
        Some(key_id) => {
            // Verify key exists eagerly at build time.
            base_provider.key(key_id).map_err(|e| {
                SaciError::configuration(format!(
                    "{context}: active_key '{key_id}' could not be resolved: {e}"
                ))
            })?;
            Arc::new(ActiveKeyOverride {
                inner: base_provider,
                active_key: key_id.to_string(),
            })
        }
        None => base_provider,
    };

    let alg_str = alg_override.or_else(|| service_enc.and_then(|c| c.algorithm.as_deref()));
    let algorithm = match alg_str {
        Some(s) => s.parse::<Algorithm>()?,
        None => Algorithm::Aes256Gcm,
    };

    Ok(Arc::new(Cipher::new(provider, algorithm)))
}
