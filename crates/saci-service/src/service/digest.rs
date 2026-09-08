//! SHA3-256 content digest for a pipeline artifact.
//!
//! One convention covers both artifact kinds: `[pipeline.wasm].sha3_256` pins a
//! WebAssembly component's bytes and `[pipeline.plugin].sha3_256` pins a shared
//! library's bytes. Both go through [`verify_sha3_256`].
//!
//! The digest hashes the artifact's file bytes and says nothing about schemas.
//! The FNV-1a `u32` schema fingerprint is a different value, computed from
//! component names, versions and field names, and the two never compare equal.

use saci_core::SaciResult;
use saci_core::error::SaciError;

/// Lowercase 64-character hex SHA3-256 of `bytes`.
pub(crate) fn sha3_256_hex(bytes: &[u8]) -> String {
    use sha3::{Digest, Sha3_256};
    let hash = Sha3_256::digest(bytes);
    use std::fmt::Write;
    let mut out = String::with_capacity(64);
    for b in hash.iter() {
        write!(out, "{b:02x}").unwrap();
    }
    out
}

/// Check the SHA3-256 of `bytes` against `expected`.
///
/// `artifact` names the thing being hashed and opens the error message, so a
/// mismatch says which file disagreed. `expected` may carry an optional
/// `sha3-256:` prefix.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] naming both digests when they differ.
pub(crate) fn verify_sha3_256(artifact: &str, bytes: &[u8], expected: &str) -> SaciResult<()> {
    // Strip optional "sha3-256:" prefix that some tooling adds.
    let expected = expected.strip_prefix("sha3-256:").unwrap_or(expected);
    let actual = sha3_256_hex(bytes);
    if actual != expected {
        return Err(SaciError::configuration(format!(
            "{artifact} SHA3-256 mismatch: expected {expected}, got {actual}"
        )));
    }
    Ok(())
}

#[cfg(all(test, feature = "service", any(feature = "wasm", feature = "plugin")))]
mod tests {
    use super::*;

    #[test]
    fn test_known_digest() {
        // SHA3-256 of the empty input, from the FIPS 202 test vectors.
        assert_eq!(
            sha3_256_hex(b""),
            "a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a"
        );
    }

    #[test]
    fn test_matching_digest_accepted() {
        let bytes = b"artifact bytes";
        let expected = sha3_256_hex(bytes);
        verify_sha3_256("test artifact", bytes, &expected).expect("digest matches");
    }

    #[test]
    fn test_prefix_is_stripped() {
        let bytes = b"artifact bytes";
        let expected = format!("sha3-256:{}", sha3_256_hex(bytes));
        verify_sha3_256("test artifact", bytes, &expected).expect("prefixed digest matches");
    }

    #[test]
    fn test_mismatch_names_artifact_and_both_digests() {
        let bytes = b"artifact bytes";
        let actual = sha3_256_hex(bytes);
        let err = verify_sha3_256("test artifact", bytes, "deadbeef").unwrap_err();
        assert_eq!(err.category(), "configuration");
        let msg = err.to_string();
        assert!(msg.contains("test artifact"), "{msg}");
        assert!(msg.contains("deadbeef"), "{msg}");
        assert!(msg.contains(&actual), "{msg}");
    }
}
