//! [`Source`]: pull-based batch ingestion into a [`Dataset`].
//!
//! A `Source` produces [`RecordBatch`]es on demand. The pipeline calls
//! [`next_batch`](Source::next_batch) in a loop until `None` is returned
//! (EOF), then appends each batch into the dataset.
//!
//! `saci-connector-channel` is the smallest implementation: an mpsc channel with
//! nothing else in the way.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;

use crate::dataset::Dataset;
use crate::error::SaciError;

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
}

/// Drain all batches from `source` into `dataset` under `component_name`.
///
/// The component must already be registered (via
/// [`register_component`](Dataset::register_component) or
/// [`register_raw_component`](Dataset::register_raw_component)) before
/// calling this function.
///
/// Returns the total number of rows appended.
///
/// # Errors
///
/// Returns the first error from `source.next_batch()` or from
/// [`Dataset::append_record_batch`].
pub async fn drain_into_dataset<S: Source + ?Sized>(
    source: &mut S,
    dataset: &mut Dataset,
    component_name: &'static str,
) -> Result<usize, SaciError> {
    let mut total = 0usize;
    while let Some(batch) = source.next_batch().await? {
        let n = batch.num_rows();
        dataset.append_record_batch(component_name, batch)?;
        total += n;
    }
    Ok(total)
}
