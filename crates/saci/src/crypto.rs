//! Authenticated data encryption for SACI payloads, codecs, and Arrow columns.
//!
//! This module complements transport-level [`crate::tls`] (`rustls`) by
//! encrypting the data itself with authenticated symmetric encryption (AEAD,
//! backed by `ring`):
//!
//! - **Whole-payload codecs** ([`EncryptedCodec`]): wraps any [`crate::codec::Codec`]
//!   so byte-carrying connectors (`file`, `s3`, `http`, `kafka`, `nats`, `tcp`,
//!   `udp`, `redb`) read and write authenticated ciphertext transparently.
//! - **Composable `Source`/`Sink` wrappers** ([`EncryptingSink`],
//!   [`DecryptingSource`]): encrypt and decrypt either selected Arrow columns
//!   ([`EncryptionMode::Fields`]) or whole [`RecordBatch`](arrow_array::RecordBatch)
//!   payloads ([`EncryptionMode::Payload`]) across any transport (`peer`,
//!   `channel`, SQL databases, or byte connectors).
//! - **Key rotation & KMS extensibility** ([`KeyProvider`], [`StaticKeyProvider`]):
//!   every ciphertext envelope carries a `key_id` header so producers rotate
//!   keys without interrupting consumers that still hold historical keys.

#![deny(missing_docs)]

mod cell;
#[cfg(feature = "codec-api")]
mod codec;
mod envelope;
mod fields;
mod io;
mod keys;
mod stream;

#[cfg(feature = "codec-api")]
pub use codec::EncryptedCodec;
pub use envelope::Cipher;
pub use fields::{
    EncryptionMode, FieldEncoding, PAYLOAD_COLUMN, decrypt_batch, encrypt_batch, wire_schema,
};
pub use io::{DecryptingSource, EncryptingSink};
pub use keys::{ActiveKey, Algorithm, KeyMaterial, KeyProvider, StaticKeyProvider};
pub use stream::EncryptingWriter;
