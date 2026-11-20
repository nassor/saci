//! Batch ingestion and egress: the [`Source`] and [`Sink`] traits, the retry
//! wrappers around them, schema casting, and the two functions that move a
//! whole component between a connector and a [`Dataset`].
//!
//! - [`Source`] yields [`RecordBatch`](arrow_array::RecordBatch)es on demand;
//!   [`Sink`] receives them one at a time.
//! - [`RetryingSource`] and [`RetryingSink`] wrap either one and retry every
//!   failed operation under a [`RetryMode`](crate::RetryMode).
//! - [`cast_batch`] casts a batch to a target schema before it is appended.
//! - [`read_all`] drains a source into a dataset component; [`write_all`]
//!   writes a dataset component to a sink.

mod cast;
mod retry;
mod sink;
mod source;

pub use cast::cast_batch;
pub use retry::{RetryingSink, RetryingSource};
pub use sink::Sink;
pub use source::Source;

use crate::dataset::Dataset;
use crate::error::SaciResult;

/// Drain every batch from `source` into `data` under `component`.
///
/// The component must already be registered (via
/// [`Dataset::register`] or [`Dataset::register_schema`]).
///
/// Returns the total number of rows appended. Does **not** call
/// [`Source::finish`]; the caller is responsible for it.
///
/// # Errors
///
/// Returns the first error from [`Source::next_batch`] or from
/// [`Dataset::append_record_batch`].
pub async fn read_all(
    source: &mut dyn Source,
    data: &mut Dataset,
    component: &str,
) -> SaciResult<usize> {
    let mut total = 0usize;
    while let Some(batch) = source.next_batch().await? {
        let n = batch.num_rows();
        data.append_record_batch(component, batch)?;
        total += n;
    }
    Ok(total)
}

/// Write all rows of `component` from `data` to `sink`.
///
/// The whole component is one contiguous batch, so this calls
/// [`Sink::write_batch`] once, or not at all when the component is empty.
/// Does **not** call [`Sink::finish`]; the caller is responsible for it.
///
/// Returns the number of rows written.
///
/// # Errors
///
/// Returns [`SaciError::ComponentNotRegistered`](crate::SaciError::ComponentNotRegistered)
/// if the component is not registered in `data`, or the first error from
/// [`Sink::write_batch`].
pub async fn write_all(sink: &mut dyn Sink, data: &Dataset, component: &str) -> SaciResult<usize> {
    let batch = data.batch_named(component)?;
    let n = batch.num_rows();
    if n == 0 {
        return Ok(0);
    }
    sink.write_batch(batch).await?;
    Ok(n)
}
