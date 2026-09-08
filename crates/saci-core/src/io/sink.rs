//! [`Sink`]: push-based batch egress from a [`Dataset`].
//!
//! A `Sink` receives [`RecordBatch`]es one at a time via
//! [`write_batch`](Sink::write_batch), then is finalised with
//! [`finish`](Sink::finish). Sinks must be finalised before dropping so that
//! buffered data is flushed.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;

use crate::dataset::Dataset;
use crate::error::SaciError;

/// Push-based batch sink for Arrow data.
///
/// Implementations need only be `Send`. Concurrent writes to the same sink are
/// not supported: exclusive `&mut self` access provides the synchronisation.
#[async_trait]
pub trait Sink: Send {
    /// Write one batch. May be called multiple times.
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError>;

    /// Flush and finalise the sink. Must be called exactly once after all
    /// batches have been written.
    async fn finish(&mut self) -> Result<(), SaciError>;

    /// The schema this sink expects. Batches must conform to this schema.
    fn schema(&self) -> Arc<Schema>;

    /// Approximate number of rows currently buffered in the sink but not yet
    /// consumed downstream.
    ///
    /// Returns `None` if the sink does not support backpressure probing. Two
    /// consumers read it, and they never share a call path: a
    /// [`Scheduler`](crate::scheduler::Scheduler) driving a bare
    /// [`Pipeline`](crate::pipeline::Pipeline) can consult it from a
    /// [`BackpressureSpec::Predicate`](crate::scheduler::BackpressureSpec)
    /// closure to skip a tick, and a host's own admission control can read a
    /// growing backlog as congestion. `saci-service`'s adaptive flow control
    /// takes it straight off the sinks it owns, which is what governs a
    /// service workflow.
    fn pending_rows(&self) -> Option<usize> {
        None
    }
}

/// Delegating impl so a boxed sink can be wrapped like a concrete one.
#[async_trait]
impl<S: Sink + ?Sized> Sink for Box<S> {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        (**self).write_batch(batch).await
    }

    async fn finish(&mut self) -> Result<(), SaciError> {
        (**self).finish().await
    }

    fn schema(&self) -> Arc<Schema> {
        (**self).schema()
    }

    fn pending_rows(&self) -> Option<usize> {
        (**self).pending_rows()
    }
}

/// Write all rows of `component_name` from `dataset` to `sink`.
///
/// Retrieves the raw [`RecordBatch`] for the component and calls
/// [`write_batch`](Sink::write_batch) once, since the whole component is one
/// contiguous `RecordBatch`. Does **not** call [`finish`](Sink::finish); the
/// caller is responsible for finalisation.
///
/// Returns the number of rows written.
///
/// # Errors
///
/// Returns `SaciError::Generic` if the component is not registered in `dataset`,
/// or the first error from `sink.write_batch()`.
pub async fn drain_dataset<K: Sink + ?Sized>(
    dataset: &Dataset,
    component_name: &'static str,
    sink: &mut K,
) -> Result<usize, SaciError> {
    let batch = dataset.batch_for(component_name).ok_or_else(|| {
        SaciError::generic(format!(
            "drain_dataset: component '{component_name}' is not registered in the dataset"
        ))
    })?;
    if batch.num_rows() == 0 {
        return Ok(0);
    }
    let n = batch.num_rows();
    sink.write_batch(batch).await?;
    Ok(n)
}
