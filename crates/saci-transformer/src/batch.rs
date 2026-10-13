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

    /// Push every byte the writer holds in its own buffers down to the handle
    /// it was opened with, without ending the stream.
    ///
    /// A sink that tracks how long the output is (to take a failed batch back
    /// out of it) calls this after each batch, so the handle's length is where
    /// the writer logically stands. The default does nothing, which is right
    /// for a format that already writes through on every batch or that holds
    /// bytes it cannot release before [`finish`](Self::finish) (parquet keeps
    /// its current row group in memory).
    ///
    /// # Errors
    ///
    /// Returns the format's flush error.
    fn flush(&mut self) -> Result<(), SaciError> {
        Ok(())
    }

    /// Whether a fresh writer opened on the same file, after earlier batches
    /// are already in it, carries on that file correctly.
    ///
    /// `true` for a format whose batches are independent of each other on the
    /// wire (ndjson, csv without a header line). `false`, the default, for a
    /// format that writes a header, schema message, footer state or dictionary
    /// once at the start of the stream. A sink recovering from a failed batch
    /// throws the writer away, because its buffers may hold half of that
    /// batch, and opens a new one only when the answer is `true`.
    fn restartable(&self) -> bool {
        false
    }

    /// Write any trailer and flush everything down to the handle this writer
    /// was opened with. Consumes the writer: a format with a footer cannot be
    /// written to afterwards.
    ///
    /// # Errors
    ///
    /// Returns the format's trailer or flush error.
    fn finish(self: Box<Self>) -> Result<(), SaciError>;
}
