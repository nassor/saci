//! Composable [`EncryptingSink`] and [`DecryptingSource`] wrappers over any [`Sink`] / [`Source`].

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;

use crate::crypto::envelope::Cipher;
use crate::crypto::fields::{EncryptionMode, decrypt_batch, encrypt_batch, wire_schema};
use crate::error::SaciError;
use crate::io::{Sink, Source};

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
                decrypt_batch(&batch, &self.plain_schema, &self.cipher, &self.mode).map(Some)
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
        self.inner.set_skip_corrupt(skip);
    }

    fn drain_corrupt(&mut self) -> Vec<crate::io::CorruptRecord> {
        self.inner.drain_corrupt()
    }
}
