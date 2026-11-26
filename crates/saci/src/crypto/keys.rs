//! Key material, AEAD algorithms, and pluggable [`KeyProvider`] rotation.

use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use ring::rand::{SecureRandom as _, SystemRandom};
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::error::SaciError;

/// Maximum UTF-8 byte length of a `key_id` stored in a wire envelope.
pub(crate) const MAX_KEY_ID_BYTES: usize = 64;

/// Supported AEAD symmetric encryption algorithms.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Algorithm {
    /// AES-256-GCM (12-byte random nonce, 16-byte authentication tag).
    #[default]
    Aes256Gcm,
    /// ChaCha20-Poly1305 (12-byte random nonce, 16-byte authentication tag).
    ChaCha20Poly1305,
}

impl Algorithm {
    /// Wire tag byte written into every envelope header.
    pub(crate) const fn wire_id(self) -> u8 {
        match self {
            Self::Aes256Gcm => 1,
            Self::ChaCha20Poly1305 => 2,
        }
    }

    /// Decode a wire tag byte into an [`Algorithm`].
    pub(crate) fn from_wire_id(id: u8) -> Result<Self, SaciError> {
        match id {
            1 => Ok(Self::Aes256Gcm),
            2 => Ok(Self::ChaCha20Poly1305),
            other => Err(SaciError::generic(format!(
                "crypto: unknown AEAD algorithm id {other}"
            ))),
        }
    }

    /// Canonical configuration string for this algorithm.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Aes256Gcm => "aes-256-gcm",
            Self::ChaCha20Poly1305 => "chacha20-poly1305",
        }
    }

    pub(crate) fn ring_algorithm(self) -> &'static ring::aead::Algorithm {
        match self {
            Self::Aes256Gcm => &ring::aead::AES_256_GCM,
            Self::ChaCha20Poly1305 => &ring::aead::CHACHA20_POLY1305,
        }
    }
}

impl FromStr for Algorithm {
    type Err = SaciError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "aes-256-gcm" | "aes256gcm" => Ok(Self::Aes256Gcm),
            "chacha20-poly1305" | "chacha20poly1305" => Ok(Self::ChaCha20Poly1305),
            other => Err(SaciError::configuration(format!(
                "crypto: unknown algorithm '{other}'; expected 'aes-256-gcm' or 'chacha20-poly1305'"
            ))),
        }
    }
}

/// 256-bit symmetric key material, zeroized on drop and redacted in [`Debug`].
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct KeyMaterial([u8; 32]);

impl std::fmt::Debug for KeyMaterial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("KeyMaterial([REDACTED])")
    }
}

impl PartialEq for KeyMaterial {
    fn eq(&self, other: &Self) -> bool {
        let mut diff = 0u8;
        for (a, b) in self.0.iter().zip(other.0.iter()) {
            diff |= a ^ b;
        }
        diff == 0
    }
}

impl Eq for KeyMaterial {}

impl KeyMaterial {
    /// Wrap a raw 32-byte key.
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Borrow the 32 raw key bytes.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Parse a 64-character hexadecimal string into a 256-bit key.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `hex` is not 64 hex digits.
    pub fn from_hex(hex: &str) -> Result<Self, SaciError> {
        let trimmed = hex.trim();
        if trimmed.len() != 64 {
            return Err(SaciError::configuration(format!(
                "crypto: hex key must be 64 hex characters (32 bytes), got {}",
                trimmed.len()
            )));
        }
        let mut out = [0u8; 32];
        for (i, chunk) in trimmed.as_bytes().as_chunks::<2>().0.iter().enumerate() {
            let pair = std::str::from_utf8(chunk)
                .map_err(|_| SaciError::configuration("crypto: hex key must be ASCII"))?;
            out[i] = u8::from_str_radix(pair, 16).map_err(|_| {
                SaciError::configuration("crypto: hex key contains non-hex characters")
            })?;
        }
        Ok(Self(out))
    }

    /// Parse a base64-encoded 32-byte key.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when decoding fails or the decoded
    /// length is not 32 bytes.
    pub fn from_base64(encoded: &str) -> Result<Self, SaciError> {
        let decoded = STANDARD
            .decode(encoded.trim())
            .map_err(|e| SaciError::configuration(format!("crypto: invalid base64 key: {e}")))?;
        let bytes: [u8; 32] = decoded.try_into().map_err(|v: Vec<u8>| {
            SaciError::configuration(format!(
                "crypto: base64 key must decode to 32 bytes, got {}",
                v.len()
            ))
        })?;
        Ok(Self(bytes))
    }

    /// Load a key from a file containing raw 32 bytes, 64 hex chars, or base64.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when the file cannot be read or
    /// does not hold a valid 256-bit key.
    pub fn from_file(path: &Path) -> Result<Self, SaciError> {
        let bytes = std::fs::read(path).map_err(|e| {
            SaciError::configuration(format!(
                "crypto: cannot read key file '{}': {e}",
                path.display()
            ))
        })?;
        if bytes.len() == 32 {
            let arr: [u8; 32] = bytes.try_into().expect("length checked");
            return Ok(Self(arr));
        }
        let text = std::str::from_utf8(&bytes).map_err(|_| {
            SaciError::configuration(format!(
                "crypto: key file '{}' is neither 32 raw bytes nor UTF-8 hex/base64",
                path.display()
            ))
        })?;
        let trimmed = text.trim();
        if trimmed.len() == 64 && trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
            Self::from_hex(trimmed)
        } else {
            Self::from_base64(trimmed)
        }
    }

    /// Generate a fresh random 256-bit key from the OS CSPRNG.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] if the OS RNG fails.
    pub fn generate() -> Result<Self, SaciError> {
        let mut bytes = [0u8; 32];
        SystemRandom::new()
            .fill(&mut bytes)
            .map_err(|_| SaciError::generic("crypto: OS random number generator failed"))?;
        Ok(Self(bytes))
    }
}

/// The active key identifier and material returned by [`KeyProvider::active_key`].
#[derive(Debug, Clone)]
pub struct ActiveKey {
    /// Key identifier written into the wire envelope header.
    pub id: String,
    /// 256-bit key material.
    pub material: KeyMaterial,
}

/// Source of encryption and decryption keys, supporting rotation and custom KMS.
pub trait KeyProvider: Send + Sync + 'static {
    /// Return the active key used to encrypt newly produced data.
    fn active_key(&self) -> Result<ActiveKey, SaciError>;

    /// Resolve the key material for `key_id` when decrypting incoming data.
    fn key(&self, key_id: &str) -> Result<KeyMaterial, SaciError>;
}

/// Validate that `id` fits in the envelope header (`1..=64` UTF-8 bytes).
pub(crate) fn validate_key_id(id: &str) -> Result<(), SaciError> {
    if id.is_empty() || id.len() > MAX_KEY_ID_BYTES {
        return Err(SaciError::configuration(format!(
            "crypto: key_id '{id}' must be 1..={MAX_KEY_ID_BYTES} UTF-8 bytes"
        )));
    }
    Ok(())
}

/// In-memory keyring supporting one active encryption key and any number of
/// historical decryption keys for seamless rotation.
#[derive(Debug, Clone)]
pub struct StaticKeyProvider {
    active_id: String,
    keys: HashMap<String, KeyMaterial>,
}

impl StaticKeyProvider {
    /// Create a provider holding a single active key.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `id` is empty or exceeds 64 bytes.
    pub fn single(id: impl Into<String>, material: KeyMaterial) -> Result<Self, SaciError> {
        let id = id.into();
        validate_key_id(&id)?;
        let mut keys = HashMap::new();
        keys.insert(id.clone(), material);
        Ok(Self {
            active_id: id,
            keys,
        })
    }

    /// Create a provider with `active_id` and a map of `key_id -> KeyMaterial`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] if any `key_id` is invalid or if
    /// `active_id` is missing from `keys`.
    pub fn new(
        active_id: impl Into<String>,
        keys: HashMap<String, KeyMaterial>,
    ) -> Result<Self, SaciError> {
        let active_id = active_id.into();
        validate_key_id(&active_id)?;
        for id in keys.keys() {
            validate_key_id(id)?;
        }
        if !keys.contains_key(&active_id) {
            return Err(SaciError::configuration(format!(
                "crypto: active key '{active_id}' is not present in key map"
            )));
        }
        Ok(Self { active_id, keys })
    }

    /// Add an additional (typically historical or staged) key for decryption.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] if `id` is invalid.
    pub fn with_key(
        mut self,
        id: impl Into<String>,
        material: KeyMaterial,
    ) -> Result<Self, SaciError> {
        let id = id.into();
        validate_key_id(&id)?;
        self.keys.insert(id, material);
        Ok(self)
    }
}

impl KeyProvider for StaticKeyProvider {
    fn active_key(&self) -> Result<ActiveKey, SaciError> {
        let material = self
            .keys
            .get(&self.active_id)
            .cloned()
            .ok_or_else(|| SaciError::generic("crypto: active key missing from keyring"))?;
        Ok(ActiveKey {
            id: self.active_id.clone(),
            material,
        })
    }

    fn key(&self, key_id: &str) -> Result<KeyMaterial, SaciError> {
        self.keys.get(key_id).cloned().ok_or_else(|| {
            SaciError::generic(format!("crypto: unknown key_id '{key_id}' in keyring"))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_material_parses_hex_base64_and_file_and_redacts_debug() {
        let key = KeyMaterial::generate().expect("generate");
        assert_eq!(format!("{key:?}"), "KeyMaterial([REDACTED])");

        let hex: String = key.as_bytes().iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(KeyMaterial::from_hex(&hex).expect("hex"), key);

        let b64 = STANDARD.encode(key.as_bytes());
        assert_eq!(KeyMaterial::from_base64(&b64).expect("b64"), key);

        let dir = tempfile::tempdir().expect("tempdir");
        let raw_path = dir.path().join("raw.key");
        std::fs::write(&raw_path, key.as_bytes()).expect("write raw");
        assert_eq!(KeyMaterial::from_file(&raw_path).expect("raw file"), key);

        let hex_path = dir.path().join("hex.key");
        std::fs::write(&hex_path, format!("{hex}\n")).expect("write hex");
        assert_eq!(KeyMaterial::from_file(&hex_path).expect("hex file"), key);

        let b64_path = dir.path().join("b64.key");
        std::fs::write(&b64_path, format!("  {b64}\r\n")).expect("write b64");
        assert_eq!(KeyMaterial::from_file(&b64_path).expect("b64 file"), key);

        // Wrong byte lengths and corrupt files fail without leaking key strings.
        let err_short = KeyMaterial::from_hex(&hex[..62]).unwrap_err();
        assert!(!err_short.to_string().contains(&hex[..16]));
        let err_b64 = KeyMaterial::from_base64("not-valid-base64!!!").unwrap_err();
        assert_eq!(err_b64.category(), "configuration");

        let short_raw = dir.path().join("short.key");
        std::fs::write(&short_raw, vec![0xAB; 31]).unwrap();
        assert!(KeyMaterial::from_file(&short_raw).is_err());

        let long_raw = dir.path().join("long.key");
        std::fs::write(&long_raw, vec![0xAB; 33]).unwrap();
        assert!(KeyMaterial::from_file(&long_raw).is_err());

        let bad_utf8 = dir.path().join("bad_utf8.key");
        std::fs::write(&bad_utf8, [0xFFu8; 33]).unwrap();
        assert!(KeyMaterial::from_file(&bad_utf8).is_err());
    }

    #[test]
    fn static_key_provider_supports_rotation_and_rejects_bad_ids() {
        let k1 = KeyMaterial::generate().unwrap();
        let k2 = KeyMaterial::generate().unwrap();
        let provider = StaticKeyProvider::single("k2", k2.clone())
            .unwrap()
            .with_key("k1", k1.clone())
            .unwrap();
        assert_eq!(provider.active_key().unwrap().id, "k2");
        assert_eq!(provider.key("k1").unwrap(), k1);
        assert_eq!(provider.key("k2").unwrap(), k2);
        assert!(provider.key("missing").is_err());
        assert!(StaticKeyProvider::single("", k1).is_err());
    }
}
