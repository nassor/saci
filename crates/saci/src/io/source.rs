//! [`Source`]: pull-based batch ingestion into a [`Dataset`](crate::dataset::Dataset).
//!
//! A `Source` produces [`RecordBatch`]es on demand. The pipeline calls
//! [`next_batch`](Source::next_batch) in a loop until `None` is returned
//! (EOF), then appends each batch into the dataset.
//!
//! The `channel` connector is the smallest implementation: an mpsc channel with
//! nothing else in the way.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;

use crate::error::SaciError;

/// One malformed or unparseable source record isolated by a decoder or reader
/// when corrupt-record recovery (`set_skip_corrupt(true)`) is enabled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorruptRecord {
    /// Byte format that rejected the record (for example `"ndjson"`, `"csv"`, or `"avro"`).
    pub format: &'static str,
    /// Raw bytes of the malformed line or message payload.
    pub payload: Vec<u8>,
    /// Parser/codec error explaining why the record was rejected.
    pub error: String,
}

/// Pull-based batch source for Arrow data.
///
/// Each call to [`next_batch`](Self::next_batch) yields the next
/// [`RecordBatch`], or `None` at EOF. Whatever size a source produces is
/// correct: a caller reshapes what it receives, and
/// [`request_batch_rows`](Self::request_batch_rows) is how it asks for a size
/// in advance. Emitting the transport's natural unit beats buffering up one
/// giant batch, which costs memory no caller asked for.
#[async_trait]
pub trait Source: Send + Sync {
    /// Arrow [`Schema`] that every batch from this source conforms to.
    ///
    /// Fixed per source instance: the schema must not change between calls.
    fn schema(&self) -> Arc<Schema>;

    /// Pull the next batch, or `None` at EOF.
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError>;

    /// Estimated total row count, or `None` if unknown.
    ///
    /// Used only for progress reporting; callers must not rely on accuracy.
    fn estimated_rows(&self) -> Option<usize> {
        None
    }

    /// Advisory hint: the number of rows the caller wants in the next batch.
    ///
    /// Advisory only. A source may return fewer rows, more rows, or ignore the
    /// hint entirely and stay correct: the caller reshapes what it receives to
    /// the size it needs. Implement it only where the underlying transport can
    /// act on it, such as a fetch limit or a poll batch size.
    fn request_batch_rows(&mut self, _rows: usize) {}

    /// Make what this source delivered durable on its side and release what
    /// it holds.
    ///
    /// Called once, after the last [`next_batch`](Self::next_batch). A source
    /// whose delivery is not a commitment (a file read, an HTTP GET) needs
    /// nothing here, which is the default. A source that consumes what it
    /// yielded (deleting the entries it handed over, committing an offset it
    /// has been holding back) does it here, so a caller that failed to process
    /// a batch can drop the source instead and see the same data again.
    async fn finish(&mut self) -> Result<(), SaciError> {
        Ok(())
    }

    /// Enable or disable per-record corrupt-input recovery when supported by
    /// the underlying source/codec.
    fn set_skip_corrupt(&mut self, _skip: bool) {}

    /// Drain malformed source records isolated since the previous call.
    fn drain_corrupt(&mut self) -> Vec<CorruptRecord> {
        Vec::new()
    }
}

/// Delegating impl so a boxed source can be wrapped like a concrete one.
#[async_trait]
impl<S: Source + ?Sized> Source for Box<S> {
    fn schema(&self) -> Arc<Schema> {
        (**self).schema()
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        (**self).next_batch().await
    }

    fn estimated_rows(&self) -> Option<usize> {
        (**self).estimated_rows()
    }

    fn request_batch_rows(&mut self, rows: usize) {
        (**self).request_batch_rows(rows);
    }

    async fn finish(&mut self) -> Result<(), SaciError> {
        (**self).finish().await
    }

    fn set_skip_corrupt(&mut self, skip: bool) {
        (**self).set_skip_corrupt(skip);
    }

    fn drain_corrupt(&mut self) -> Vec<CorruptRecord> {
        (**self).drain_corrupt()
    }
}
