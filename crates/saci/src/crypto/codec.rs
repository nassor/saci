//! [`EncryptedCodec`]: transparent whole-payload encryption around any [`Codec`].

use std::io::Write;
use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use arrow_schema::Schema;

use crate::codec::{
    BatchReader, BatchWriter, Codec, CorruptRecord, MessageDecoder, MessageShape, ReadInput,
};
use crate::crypto::envelope::Cipher;
use crate::crypto::stream::{EncryptingWriter, decrypt_read_input};
use crate::error::SaciError;

const MESSAGE_AAD: &[u8] = b"saci:msg";

/// Wraps any [`Codec`] (`arrow-ipc`, `avro`, `csv`, `ndjson`, `parquet`) so
/// both its stream surface and its discrete message surface read and write
/// authenticated AEAD ciphertext.
#[derive(Clone)]
pub struct EncryptedCodec {
    inner: Arc<dyn Codec>,
    cipher: Arc<Cipher>,
}

impl std::fmt::Debug for EncryptedCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptedCodec")
            .field("format", &self.inner.format())
            .field("cipher", &self.cipher)
            .finish()
    }
}

impl EncryptedCodec {
    /// Wrap `inner` with AEAD encryption/decryption using `cipher`.
    pub fn new(inner: Arc<dyn Codec>, cipher: Arc<Cipher>) -> Self {
        Self { inner, cipher }
    }

    /// Borrow the underlying plaintext codec.
    pub fn inner(&self) -> &Arc<dyn Codec> {
        &self.inner
    }

    /// Borrow the cipher protecting this codec.
    pub fn cipher(&self) -> &Arc<Cipher> {
        &self.cipher
    }
}

/// Shared handle letting `EncryptedBatchWriter` finalize the encrypting stream
/// after the inner `BatchWriter` writes its footer and drops.
type SharedWriter = Arc<Mutex<Option<EncryptingWriter<Box<dyn Write + Send>>>>>;

struct SharedEncryptingWrite {
    shared: SharedWriter,
}

impl Write for SharedEncryptingWrite {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut guard = self
            .shared
            .lock()
            .map_err(|_| std::io::Error::other("encrypted writer lock poisoned"))?;
        let writer = guard
            .as_mut()
            .ok_or_else(|| std::io::Error::other("encrypted writer already finished"))?;
        writer.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut guard = self
            .shared
            .lock()
            .map_err(|_| std::io::Error::other("encrypted writer lock poisoned"))?;
        if let Some(writer) = guard.as_mut() {
            writer.flush_chunk()?;
        }
        Ok(())
    }
}

struct EncryptedBatchWriter {
    inner: Box<dyn BatchWriter>,
    shared: SharedWriter,
}

impl BatchWriter for EncryptedBatchWriter {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        self.inner.write_batch(batch)
    }

    fn flush(&mut self) -> Result<(), SaciError> {
        self.inner.flush()?;
        let mut guard = self
            .shared
            .lock()
            .map_err(|_| SaciError::generic("crypto: encrypted writer lock poisoned"))?;
        if let Some(writer) = guard.as_mut() {
            writer.flush_chunk().map_err(|e| {
                SaciError::generic(format!("crypto: flushing encrypted writer: {e}"))
            })?;
        }
        Ok(())
    }

    fn restartable(&self) -> bool {
        false
    }

    fn finish(self: Box<Self>) -> Result<(), SaciError> {
        let Self { inner, shared } = *self;
        inner.finish()?;
        let writer = shared
            .lock()
            .map_err(|_| SaciError::generic("crypto: encrypted writer lock poisoned"))?
            .take()
            .ok_or_else(|| SaciError::generic("crypto: encrypted writer already finished"))?;
        writer.finish()?;
        Ok(())
    }
}

struct DecryptingMessageDecoder {
    inner: Box<dyn MessageDecoder>,
    cipher: Arc<Cipher>,
    format: &'static str,
    skip_corrupt: bool,
    corrupt: Vec<CorruptRecord>,
}

impl MessageDecoder for DecryptingMessageDecoder {
    fn set_skip_corrupt(&mut self, skip: bool) {
        self.skip_corrupt = skip;
        self.inner.set_skip_corrupt(skip);
    }

    fn drain_corrupt(&mut self) -> Vec<CorruptRecord> {
        let mut out = std::mem::take(&mut self.corrupt);
        out.extend(self.inner.drain_corrupt());
        out
    }

    fn push(&mut self, payload: &[u8]) -> Result<(), SaciError> {
        let plain = match self.cipher.decrypt(payload, MESSAGE_AAD) {
            Ok(plain) => plain,
            Err(err) if self.skip_corrupt => {
                self.corrupt.push(CorruptRecord {
                    format: self.format,
                    payload: payload.to_vec(),
                    error: err.to_string(),
                });
                return Ok(());
            }
            Err(err) => return Err(err),
        };
        self.inner.push(&plain)
    }

    fn flush(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        self.inner.flush()
    }
}

impl Codec for EncryptedCodec {
    fn format(&self) -> &'static str {
        self.inner.format()
    }

    fn open_reader(
        &self,
        input: ReadInput,
        declared: Option<Arc<Schema>>,
    ) -> Result<Box<dyn BatchReader>, SaciError> {
        let decrypted = decrypt_read_input(input, &self.cipher)?;
        self.inner.open_reader(decrypted, declared)
    }

    fn open_writer(
        &self,
        output: Box<dyn Write + Send>,
        schema: Arc<Schema>,
    ) -> Result<Box<dyn BatchWriter>, SaciError> {
        let enc_writer = EncryptingWriter::new(output, Arc::clone(&self.cipher));
        let shared: SharedWriter = Arc::new(Mutex::new(Some(enc_writer)));
        let handle = Box::new(SharedEncryptingWrite {
            shared: Arc::clone(&shared),
        });
        let inner = self.inner.open_writer(handle, schema)?;
        Ok(Box::new(EncryptedBatchWriter { inner, shared }))
    }

    fn open_message_decoder(
        &self,
        schema: Arc<Schema>,
    ) -> Result<Box<dyn MessageDecoder>, SaciError> {
        let inner = self.inner.open_message_decoder(schema)?;
        Ok(Box::new(DecryptingMessageDecoder {
            inner,
            cipher: Arc::clone(&self.cipher),
            format: self.inner.format(),
            skip_corrupt: false,
            corrupt: Vec::new(),
        }))
    }

    fn encode_messages(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>, SaciError> {
        let plain_msgs = self.inner.encode_messages(batch)?;
        let mut encrypted = Vec::with_capacity(plain_msgs.len());
        for msg in plain_msgs {
            encrypted.push(self.cipher.encrypt(&msg, MESSAGE_AAD)?);
        }
        Ok(encrypted)
    }

    fn message_shape(&self) -> Option<MessageShape> {
        self.inner.message_shape()
    }

    fn media_type(&self) -> Option<&'static str> {
        Some("application/octet-stream")
    }
}

#[cfg(all(test, feature = "codec-arrow-ipc"))]
mod tests {
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field};

    use super::*;
    use crate::codec::arrow_ipc::ArrowIpcCodec;
    use crate::crypto::keys::{KeyMaterial, StaticKeyProvider};

    fn test_codec() -> EncryptedCodec {
        let provider =
            Arc::new(StaticKeyProvider::single("k1", KeyMaterial::generate().unwrap()).unwrap());
        let cipher = Arc::new(Cipher::aes_256_gcm(provider));
        EncryptedCodec::new(Arc::new(ArrowIpcCodec::new()), cipher)
    }

    #[test]
    fn stream_and_message_surfaces_encrypt_and_decrypt() {
        let codec = test_codec();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![10, 20, 30]))],
        )
        .unwrap();

        let shared_buf = Arc::new(Mutex::new(Vec::<u8>::new()));
        struct BufWrite(Arc<Mutex<Vec<u8>>>);
        impl Write for BufWrite {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let mut writer = codec
            .open_writer(Box::new(BufWrite(Arc::clone(&shared_buf))), schema.clone())
            .unwrap();
        assert!(!writer.restartable());
        writer.write_batch(&batch).unwrap();
        writer.finish().unwrap();

        let bytes = shared_buf.lock().unwrap().clone();
        assert!(bytes.starts_with(b"SACS"));
        assert!(
            ArrowIpcCodec::new()
                .open_reader(ReadInput::from(bytes.clone()), Some(schema.clone()))
                .is_err(),
            "plaintext codec must reject encrypted stream"
        );

        let mut reader = codec
            .open_reader(ReadInput::from(bytes), Some(schema.clone()))
            .unwrap();
        let out = reader.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 3);

        // Message surface.
        let msgs = codec.encode_messages(&batch).unwrap();
        assert_eq!(msgs.len(), 1);
        assert!(msgs[0].starts_with(b"SACI"));
        let mut decoder = codec.open_message_decoder(schema).unwrap();
        decoder.push(&msgs[0]).unwrap();
        let decoded = decoder.flush().unwrap().unwrap();
        assert_eq!(decoded.num_rows(), 3);
    }

    #[cfg(feature = "codec-ndjson")]
    #[test]
    fn decrypting_message_decoder_isolates_corrupt_ciphertext_and_inner_records() {
        let provider =
            Arc::new(StaticKeyProvider::single("k1", KeyMaterial::generate().unwrap()).unwrap());
        let cipher = Arc::new(Cipher::aes_256_gcm(provider));
        let codec = EncryptedCodec::new(Arc::new(crate::codec::ndjson::NdjsonCodec::new()), cipher);

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![10]))],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![20]))],
        )
        .unwrap();

        let msg1 = codec.encode_messages(&batch1).unwrap().pop().unwrap();
        let msg2 = codec.encode_messages(&batch2).unwrap().pop().unwrap();

        let mut decoder = codec.open_message_decoder(Arc::clone(&schema)).unwrap();
        decoder.set_skip_corrupt(true);
        decoder.push(&msg1).unwrap();
        // Corrupt AEAD ciphertext (wrong payload / auth tag mismatch).
        decoder
            .push(b"not-valid-ciphertext")
            .expect("corrupt ciphertext must be isolated");
        // Valid outer ciphertext whose inner NDJSON payload is malformed.
        let bad_inner = codec
            .cipher
            .encrypt(b"{not-valid-json", MESSAGE_AAD)
            .unwrap();
        decoder
            .push(&bad_inner)
            .expect("inner corrupt payload must be isolated");
        decoder.push(&msg2).unwrap();

        let decoded = decoder.flush().unwrap().expect("two valid rows decoded");
        assert_eq!(
            decoded
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[10, 20]
        );
        let corrupt = decoder.drain_corrupt();
        assert_eq!(corrupt.len(), 2);
        assert_eq!(corrupt[0].format, "ndjson");
        assert_eq!(corrupt[0].payload, b"not-valid-ciphertext");
        assert_eq!(corrupt[1].format, "ndjson");
        assert_eq!(corrupt[1].payload, b"{not-valid-json");
    }
}
