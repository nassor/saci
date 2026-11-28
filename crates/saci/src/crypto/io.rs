//! Composable [`EncryptingSink`] and [`DecryptingSource`] wrappers over any [`Sink`] / [`Source`].

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;

use crate::crypto::envelope::Cipher;
use crate::crypto::fields::{
    EncryptionMode, decrypt_batch, decrypt_batch_isolating_corrupt, encrypt_batch, wire_schema,
};
use crate::error::SaciError;
use crate::io::{CorruptRecord, Sink, Source};

/// Wraps an inner [`Sink`], encrypting configured fields or the whole batch
/// before delegating each write.
///
/// [`Sink::schema`] reports `plain_schema`, while `inner` receives batches in
/// `wire_schema(&plain_schema, &mode)`.
pub struct EncryptingSink<S: Sink> {
    inner: S,
    plain_schema: Arc<Schema>,
    wire_schema: Arc<Schema>,
    cipher: Arc<Cipher>,
    mode: EncryptionMode,
}

impl<S: Sink> EncryptingSink<S> {
    /// Wrap `inner`, validating that `inner.schema()` matches the wire schema
    /// derived from `plain_schema` and `mode`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `mode` is invalid for
    /// `plain_schema` or when `inner.schema().fields()` disagrees with the
    /// derived wire schema.
    pub fn new(
        inner: S,
        plain_schema: Arc<Schema>,
        cipher: Arc<Cipher>,
        mode: EncryptionMode,
    ) -> Result<Self, SaciError> {
        let wire = wire_schema(&plain_schema, &mode)?;
        if inner.schema().fields() != wire.fields() {
            return Err(SaciError::configuration(format!(
                "EncryptingSink: inner sink schema {:?} does not match derived wire schema {:?}",
                inner.schema().fields(),
                wire.fields()
            )));
        }
        Ok(Self {
            inner,
            plain_schema,
            wire_schema: wire,
            cipher,
            mode,
        })
    }
}

#[async_trait]
impl<S: Sink> Sink for EncryptingSink<S> {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        if batch.schema().fields() != self.plain_schema.fields() {
            return Err(SaciError::configuration(format!(
                "EncryptingSink: batch schema {:?} does not match expected plain schema {:?}",
                batch.schema().fields(),
                self.plain_schema.fields()
            )));
        }
        let encrypted = encrypt_batch(batch, &self.wire_schema, &self.cipher, &self.mode)?;
        self.inner.write_batch(&encrypted).await
    }

    async fn finish(&mut self) -> Result<(), SaciError> {
        self.inner.finish().await
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.plain_schema)
    }

    fn pending_rows(&self) -> Option<usize> {
        self.inner.pending_rows()
    }
}

/// Wraps an inner [`Source`], decrypting configured fields or whole-batch
/// envelopes into `plain_schema` as batches arrive.
pub struct DecryptingSource<S: Source> {
    inner: S,
    plain_schema: Arc<Schema>,
    cipher: Arc<Cipher>,
    mode: EncryptionMode,
    skip_corrupt: bool,
    corrupt: Vec<CorruptRecord>,
}

impl<S: Source> DecryptingSource<S> {
    /// Wrap `inner`, validating that `inner.schema()` matches the wire schema
    /// derived from `plain_schema` and `mode`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `mode` is invalid for
    /// `plain_schema` or when `inner.schema().fields()` disagrees with the
    /// derived wire schema.
    pub fn new(
        inner: S,
        plain_schema: Arc<Schema>,
        cipher: Arc<Cipher>,
        mode: EncryptionMode,
    ) -> Result<Self, SaciError> {
        let wire = wire_schema(&plain_schema, &mode)?;
        if inner.schema().fields() != wire.fields() {
            return Err(SaciError::configuration(format!(
                "DecryptingSource: inner source schema {:?} does not match derived wire schema {:?}",
                inner.schema().fields(),
                wire.fields()
            )));
        }
        Ok(Self {
            inner,
            plain_schema,
            cipher,
            mode,
            skip_corrupt: false,
            corrupt: Vec::new(),
        })
    }
}

#[async_trait]
impl<S: Source> Source for DecryptingSource<S> {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.plain_schema)
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        match self.inner.next_batch().await? {
            Some(batch) => {
                if self.skip_corrupt {
                    let (decrypted, corrupt) = decrypt_batch_isolating_corrupt(
                        &batch,
                        &self.plain_schema,
                        &self.cipher,
                        &self.mode,
                    )?;
                    self.corrupt.extend(corrupt);
                    Ok(Some(decrypted))
                } else {
                    decrypt_batch(&batch, &self.plain_schema, &self.cipher, &self.mode).map(Some)
                }
            }
            None => Ok(None),
        }
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.inner.estimated_rows()
    }

    fn request_batch_rows(&mut self, rows: usize) {
        self.inner.request_batch_rows(rows);
    }

    async fn finish(&mut self) -> Result<(), SaciError> {
        self.inner.finish().await
    }

    fn set_skip_corrupt(&mut self, skip: bool) {
        self.skip_corrupt = skip;
        self.inner.set_skip_corrupt(skip);
    }

    fn drain_corrupt(&mut self) -> Vec<CorruptRecord> {
        let mut drained = self.inner.drain_corrupt();
        drained.append(&mut self.corrupt);
        drained
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field};
    use tokio::sync::Mutex;

    use super::*;
    use crate::crypto::fields::FieldEncoding;
    use crate::crypto::keys::{KeyMaterial, StaticKeyProvider};

    struct VecSource {
        schema: Arc<Schema>,
        batches: std::collections::VecDeque<RecordBatch>,
        inner_corrupt: Arc<Mutex<Vec<CorruptRecord>>>,
    }

    #[async_trait]
    impl Source for VecSource {
        fn schema(&self) -> Arc<Schema> {
            Arc::clone(&self.schema)
        }

        async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
            Ok(self.batches.pop_front())
        }

        fn drain_corrupt(&mut self) -> Vec<CorruptRecord> {
            self.inner_corrupt
                .try_lock()
                .map(|mut g| std::mem::take(&mut *g))
                .unwrap_or_default()
        }
    }

    #[tokio::test]
    async fn decrypting_source_isolates_corrupt_encrypted_rows_when_skip_corrupt_enabled() {
        let provider =
            Arc::new(StaticKeyProvider::single("k1", KeyMaterial::generate().unwrap()).unwrap());
        let cipher = Arc::new(Cipher::aes_256_gcm(provider));
        let plain_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("secret", DataType::Utf8, true),
        ]));
        let mode = EncryptionMode::fields_with_encoding(["secret"], FieldEncoding::Base64Utf8);
        let wire = wire_schema(&plain_schema, &mode).unwrap();

        let plain = RecordBatch::try_new(
            Arc::clone(&plain_schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    Some("alpha"),
                    Some("beta"),
                    Some("gamma"),
                ])),
            ],
        )
        .unwrap();
        let encrypted = encrypt_batch(&plain, &wire, &cipher, &mode).unwrap();
        let enc_col = encrypted
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let tampered_col = Arc::new(StringArray::from(vec![
            Some(enc_col.value(0)),
            Some("not!valid!base64"),
            Some(enc_col.value(2)),
        ]));
        let tampered_batch = RecordBatch::try_new(
            Arc::clone(&wire),
            vec![Arc::clone(encrypted.column(0)), tampered_col],
        )
        .unwrap();

        let inner_corrupt = Arc::new(Mutex::new(vec![CorruptRecord {
            format: "ndjson",
            payload: b"{bad json".to_vec(),
            error: "inner parse error".to_string(),
        }]));
        let inner = VecSource {
            schema: wire,
            batches: std::collections::VecDeque::from([tampered_batch]),
            inner_corrupt,
        };
        let mut src = DecryptingSource::new(inner, plain_schema, cipher, mode).unwrap();
        src.set_skip_corrupt(true);

        let batch = src
            .next_batch()
            .await
            .expect("decrypt survives corrupt row")
            .expect("batch present");
        assert_eq!(batch.num_rows(), 2);
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.values(), &[1, 3]);

        let drained = src.drain_corrupt();
        assert_eq!(
            drained.len(),
            2,
            "must combine inner and crypto corrupt records"
        );
        assert_eq!(drained[0].format, "ndjson");
        assert_eq!(drained[1].format, "crypto");
        assert_eq!(drained[1].payload, b"not!valid!base64");
    }
}
