//! Self-describing AEAD envelope encoding and [`Cipher`] operations.

use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey};
use ring::rand::{SecureRandom as _, SystemRandom};

use crate::crypto::keys::{Algorithm, KeyProvider, MAX_KEY_ID_BYTES, validate_key_id};
use crate::error::SaciError;

/// Envelope magic header.
const MAGIC: [u8; 4] = *b"SACI";
/// Envelope format version.
const VERSION: u8 = 1;
/// 96-bit AEAD nonce length (AES-256-GCM and ChaCha20-Poly1305).
const NONCE_LEN: usize = 12;
/// 128-bit AEAD authentication tag length.
const TAG_LEN: usize = 16;
/// Fixed header prefix size: magic(4) + version(1) + alg(1) + key_id_len(1).
const HEADER_PREFIX_LEN: usize = 7;

/// AEAD encryption and decryption engine backed by a [`KeyProvider`].
///
/// Every encrypted blob uses a self-describing header (`SACI`, version,
/// algorithm, `key_id`, and random 96-bit nonce) authenticated as AAD alongside
/// caller-supplied context AAD.
#[derive(Clone)]
pub struct Cipher {
    provider: Arc<dyn KeyProvider>,
    algorithm: Algorithm,
    rng: SystemRandom,
}

impl std::fmt::Debug for Cipher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cipher")
            .field("algorithm", &self.algorithm)
            .finish_non_exhaustive()
    }
}

impl Cipher {
    /// Construct a cipher using `provider` and `algorithm`.
    pub fn new(provider: Arc<dyn KeyProvider>, algorithm: Algorithm) -> Self {
        Self {
            provider,
            algorithm,
            rng: SystemRandom::new(),
        }
    }

    /// Construct an AES-256-GCM cipher.
    pub fn aes_256_gcm(provider: Arc<dyn KeyProvider>) -> Self {
        Self::new(provider, Algorithm::Aes256Gcm)
    }

    /// Construct a ChaCha20-Poly1305 cipher.
    pub fn chacha20_poly1305(provider: Arc<dyn KeyProvider>) -> Self {
        Self::new(provider, Algorithm::ChaCha20Poly1305)
    }

    /// Algorithm used for encrypting new payloads.
    pub const fn algorithm(&self) -> Algorithm {
        self.algorithm
    }

    /// Key provider backing this cipher.
    pub fn provider(&self) -> &Arc<dyn KeyProvider> {
        &self.provider
    }

    /// Encrypt `plaintext` with `aad` context binding and return a binary envelope.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError`] if the active key cannot be resolved, `key_id` is
    /// invalid, or encryption fails.
    pub fn encrypt(&self, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>, SaciError> {
        let active = self.provider.active_key()?;
        validate_key_id(&active.id)?;
        let key_id = active.id.as_bytes();

        let mut nonce_bytes = [0u8; NONCE_LEN];
        self.rng
            .fill(&mut nonce_bytes)
            .map_err(|_| SaciError::generic("crypto: OS random number generator failed"))?;

        let header_len = HEADER_PREFIX_LEN + key_id.len();
        let mut out = Vec::with_capacity(header_len + NONCE_LEN + plaintext.len() + TAG_LEN);
        out.extend_from_slice(&MAGIC);
        out.push(VERSION);
        out.push(self.algorithm.wire_id());
        out.push(key_id.len() as u8);
        out.extend_from_slice(key_id);
        out.extend_from_slice(&nonce_bytes);

        let full_aad = build_aad(&out[..header_len], aad);
        let payload_start = out.len();
        out.extend_from_slice(plaintext);

        let unbound = UnboundKey::new(self.algorithm.ring_algorithm(), active.material.as_bytes())
            .map_err(|_| SaciError::generic("crypto: invalid encryption key length"))?;
        let key = LessSafeKey::new(unbound);
        let nonce = Nonce::assume_unique_for_key(nonce_bytes);

        let mut in_out = out.split_off(payload_start);
        key.seal_in_place_append_tag(nonce, Aad::from(&full_aad), &mut in_out)
            .map_err(|_| SaciError::generic("crypto: AEAD seal failed"))?;
        out.extend_from_slice(&in_out);
        Ok(out)
    }

    /// Decrypt and authenticate a binary `envelope` against `aad`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] if the envelope is malformed, names an
    /// unknown `key_id`, or fails AEAD authentication.
    pub fn decrypt(&self, envelope: &[u8], aad: &[u8]) -> Result<Vec<u8>, SaciError> {
        if envelope.len() < HEADER_PREFIX_LEN + NONCE_LEN + TAG_LEN {
            return Err(SaciError::generic(
                "crypto: encrypted envelope is truncated",
            ));
        }
        if envelope[..4] != MAGIC {
            return Err(SaciError::generic(
                "crypto: invalid encrypted envelope magic header",
            ));
        }
        if envelope[4] != VERSION {
            return Err(SaciError::generic(format!(
                "crypto: unsupported envelope version {}",
                envelope[4]
            )));
        }
        let alg = Algorithm::from_wire_id(envelope[5])?;
        let key_id_len = envelope[6] as usize;
        if key_id_len == 0 || key_id_len > MAX_KEY_ID_BYTES {
            return Err(SaciError::generic(format!(
                "crypto: invalid key_id length {key_id_len} in envelope"
            )));
        }
        let header_len = HEADER_PREFIX_LEN + key_id_len;
        if envelope.len() < header_len + NONCE_LEN + TAG_LEN {
            return Err(SaciError::generic(
                "crypto: encrypted envelope is truncated",
            ));
        }
        let key_id = std::str::from_utf8(&envelope[HEADER_PREFIX_LEN..header_len])
            .map_err(|_| SaciError::generic("crypto: non-UTF8 key_id in envelope"))?;

        let material = self.provider.key(key_id)?;
        let nonce_bytes: [u8; NONCE_LEN] = envelope[header_len..header_len + NONCE_LEN]
            .try_into()
            .expect("nonce slice length checked");
        let full_aad = build_aad(&envelope[..header_len], aad);

        let unbound = UnboundKey::new(alg.ring_algorithm(), material.as_bytes())
            .map_err(|_| SaciError::generic("crypto: invalid decryption key length"))?;
        let key = LessSafeKey::new(unbound);
        let nonce = Nonce::assume_unique_for_key(nonce_bytes);

        let mut buf = envelope[header_len + NONCE_LEN..].to_vec();
        let plain = key
            .open_in_place(nonce, Aad::from(&full_aad), &mut buf)
            .map_err(|_| SaciError::generic("crypto: AEAD authentication failed"))?;
        Ok(plain.to_vec())
    }

    /// Encrypt `plaintext` and encode the resulting envelope as standard base64.
    ///
    /// # Errors
    ///
    /// Propagates any error from [`encrypt`](Self::encrypt).
    pub fn encrypt_base64(&self, plaintext: &[u8], aad: &[u8]) -> Result<String, SaciError> {
        let bytes = self.encrypt(plaintext, aad)?;
        Ok(STANDARD.encode(bytes))
    }

    /// Decode base64 `encoded` and decrypt the resulting envelope against `aad`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] on invalid base64 or decryption failure.
    pub fn decrypt_base64(&self, encoded: &str, aad: &[u8]) -> Result<Vec<u8>, SaciError> {
        let bytes = STANDARD
            .decode(encoded.trim())
            .map_err(|e| SaciError::generic(format!("crypto: invalid base64 ciphertext: {e}")))?;
        self.decrypt(&bytes, aad)
    }
}

fn build_aad(header: &[u8], context: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(header.len() + context.len());
    out.extend_from_slice(header);
    out.extend_from_slice(context);
    out
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::crypto::keys::{KeyMaterial, StaticKeyProvider};

    #[test]
    fn aes_and_chacha_round_trip_and_tamper_detection() {
        let k1 = KeyMaterial::generate().unwrap();
        let k2 = KeyMaterial::generate().unwrap();
        let writer_provider = Arc::new(StaticKeyProvider::single("k1", k1.clone()).unwrap());

        for alg in [Algorithm::Aes256Gcm, Algorithm::ChaCha20Poly1305] {
            let cipher = Cipher::new(writer_provider.clone(), alg);
            let ct = cipher.encrypt(b"hello saci", b"ctx-a").unwrap();
            assert_eq!(cipher.decrypt(&ct, b"ctx-a").unwrap(), b"hello saci");

            // Wrong AAD fails.
            assert!(cipher.decrypt(&ct, b"ctx-b").is_err());

            // Tampered payload fails.
            let mut tampered = ct.clone();
            let last = tampered.len() - 1;
            tampered[last] ^= 0xFF;
            assert!(cipher.decrypt(&tampered, b"ctx-a").is_err());

            // Tampered header algorithm byte fails.
            let mut bad_hdr = ct.clone();
            bad_hdr[5] = if alg == Algorithm::Aes256Gcm { 2 } else { 1 };
            assert!(cipher.decrypt(&bad_hdr, b"ctx-a").is_err());

            // Base64 round-trip.
            let b64 = cipher.encrypt_base64(b"secret", b"col").unwrap();
            assert_eq!(cipher.decrypt_base64(&b64, b"col").unwrap(), b"secret");
        }

        // Key rotation: consumer on active k2 still decrypts k1.
        let rotated = Arc::new(
            StaticKeyProvider::single("k2", k2)
                .unwrap()
                .with_key("k1", k1.clone())
                .unwrap(),
        );
        let enc_old = Cipher::aes_256_gcm(writer_provider);
        let dec_new = Cipher::chacha20_poly1305(rotated);
        let env = enc_old.encrypt(b"rotated payload", b"aad").unwrap();
        assert_eq!(dec_new.decrypt(&env, b"aad").unwrap(), b"rotated payload");

        // Header and structural hardening checks.
        let mut wrong_magic = env.clone();
        wrong_magic[0] ^= 0x01;
        assert!(dec_new.decrypt(&wrong_magic, b"aad").is_err());

        let mut wrong_version = env.clone();
        wrong_version[4] = 99;
        assert!(dec_new.decrypt(&wrong_version, b"aad").is_err());

        let mut unknown_alg = env.clone();
        unknown_alg[5] = 42;
        assert!(dec_new.decrypt(&unknown_alg, b"aad").is_err());

        let mut zero_key_id = env.clone();
        zero_key_id[6] = 0;
        assert!(dec_new.decrypt(&zero_key_id, b"aad").is_err());

        let mut oversized_key_id = env.clone();
        oversized_key_id[6] = 65;
        assert!(dec_new.decrypt(&oversized_key_id, b"aad").is_err());

        // Non-UTF8 key_id.
        let mut non_utf8_key_id = env.clone();
        non_utf8_key_id[6] = 2;
        non_utf8_key_id[7] = 0xFF;
        non_utf8_key_id[8] = 0xFE;
        assert!(dec_new.decrypt(&non_utf8_key_id, b"aad").is_err());

        // Nonce bit-flip (starts at header_len = 7 + 2 = 9).
        let mut flipped_nonce = env.clone();
        flipped_nonce[9] ^= 0x80;
        assert!(dec_new.decrypt(&flipped_nonce, b"aad").is_err());

        // Truncated frames of every length below minimum.
        for len in 0..env.len() {
            assert!(dec_new.decrypt(&env[..len], b"aad").is_err());
        }

        // Empty plaintext encrypts/decrypts properly and key_id of len 64 works.
        let max_id = "k".repeat(64);
        let max_provider = Arc::new(StaticKeyProvider::single(max_id, k1).unwrap());
        let max_cipher = Cipher::aes_256_gcm(max_provider);
        let empty_ct = max_cipher.encrypt(b"", b"empty-aad").unwrap();
        assert_eq!(max_cipher.decrypt(&empty_ct, b"empty-aad").unwrap(), b"");
    }
}
