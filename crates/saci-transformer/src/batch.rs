//! The stream surface: [`BatchReader`] and [`BatchWriter`].
//!
//! Both are synchronous. The connector runs them on a blocking thread and owns
//! the channel that carries batches back to the async executor, so a byte
//! format never depends on an async runtime.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;

use saci_core::error::SaciError;

/// Pull batches out of a byte stream. Runs on a blocking thread, so it is
/// synchronous: the connector owns the async plumbing.
pub trait BatchReader: Send {
    /// The schema every batch this reader yields conforms to.
    fn schema(&self) -> Arc<Schema>;

    /// The next batch, or `None` at end of stream.
    ///
    /// # Errors
    ///
    /// Returns the format's decode error, named after the format so the
    /// connector's own message stays about transport.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError>;

    /// Estimated total rows, when the format records one.
    fn estimated_rows(&self) -> Option<usize> {
        None
    }
}

/// Push batches into a byte stream.
pub trait BatchWriter: Send {
    /// Write one batch.
    ///
    /// # Errors
    ///
    /// Returns the format's encode error.
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError>;

    /// Write any trailer and flush everything down to the handle this writer
    /// was opened with. Consumes the writer: a format with a footer cannot be
    /// written to afterwards.
    ///
    /// # Errors
    ///
    /// Returns the format's trailer or flush error.
    fn finish(self: Box<Self>) -> Result<(), SaciError>;
}
