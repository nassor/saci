//! Window aggregation results container.
//!
//! [`WindowResults`] is an ephemeral container stored as a
//! [`Resource`](crate::pipeline::Pipeline) after a
//! [`WindowedSystem`](super::system::WindowedSystem) runs. Downstream systems
//! consume it; it is not persisted via IPC.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;

/// A side-output container for rows routed away from the main aggregation path,
/// such as late data beyond the allowed-lateness window.
///
/// `T` is an opaque tag that keeps separate side-output channels apart, for
/// instance a unit struct per channel. [`DroppedLate`] is the built-in tag for
/// rows discarded once the lateness budget is exceeded.
///
/// # Example
///
/// ```
/// # #[cfg(feature = "windows")]
/// # {
/// use saci_core::windows::result::{DroppedLate, SideOutput};
///
/// let mut out: SideOutput<DroppedLate> = SideOutput::new();
/// // batches can be pushed by the windowed system and inspected downstream
/// assert!(out.is_empty());
/// # }
/// ```
#[derive(Debug)]
pub struct SideOutput<T> {
    /// Tag type identifying this side-output channel.
    _tag: std::marker::PhantomData<T>,
    /// Raw record batches routed to this channel.
    pub batches: Vec<RecordBatch>,
}

impl<T> SideOutput<T> {
    /// Create an empty side-output channel.
    pub fn new() -> Self {
        Self {
            _tag: std::marker::PhantomData,
            batches: Vec::new(),
        }
    }

    /// Push a batch into this side-output channel.
    pub fn push(&mut self, batch: RecordBatch) {
        self.batches.push(batch);
    }

    /// Total rows across all batches in this channel.
    pub fn total_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// `true` when no batches have been pushed.
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }
}

impl<T> Default for SideOutput<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Tag for the built-in side-output channel that receives rows dropped because
/// their event-timestamp exceeded the allowed-lateness budget.
///
/// Stored as a resource in the pipeline: `pipeline.get_resource::<SideOutput<DroppedLate>>()`.
#[derive(Debug, Clone, Copy)]
pub struct DroppedLate;

/// Ephemeral container for windowed aggregation output.
///
/// Produced by [`WindowedSystem`](super::system::WindowedSystem) and inserted
/// into the dataset as a resource via
/// [`Dataset::insert_resource`](crate::dataset::Dataset::insert_resource).
///
/// Each run replaces the previous `WindowResults` with the **finalized windows
/// for that run only**. Cross-run accumulation belongs to the
/// [`WindowAccumulator`](super::accumulator::WindowAccumulator) component, which
/// the system updates and the distributed runner checkpoints.
#[derive(Debug)]
pub struct WindowResults {
    /// Schema shared by all result batches.
    ///
    /// For `ReduceAggregate::Sum` this contains three fields:
    /// `window_id` (Int64), `key_hash` (Int64), and the aggregated value
    /// column (same numeric type as the source field).
    pub schema: Arc<Schema>,

    /// Result record batches for on-time window firings, one per aggregated group.
    ///
    /// Each batch has exactly one row. All batches share `schema`.
    pub batches: Vec<RecordBatch>,

    /// Result batches from late-data re-firings (within the allowed-lateness window).
    ///
    /// Each batch has exactly one row. All batches share `schema`.
    pub late_batches: Vec<RecordBatch>,

    /// Rows routed to the side-output because their timestamp exceeded the
    /// allowed-lateness budget.
    pub side_output: SideOutput<DroppedLate>,
}

impl WindowResults {
    /// Create a new `WindowResults` with the given schema and no batches.
    pub fn new(schema: Arc<Schema>) -> Self {
        Self {
            schema,
            batches: Vec::new(),
            late_batches: Vec::new(),
            side_output: SideOutput::new(),
        }
    }

    /// Total number of on-time output rows across all result batches.
    pub fn total_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Total rows including both on-time and late re-firings.
    pub fn total_rows_including_late(&self) -> usize {
        self.total_rows()
            + self
                .late_batches
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>()
    }

    /// `true` when no on-time result batches have been produced.
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array};
    use arrow_schema::{DataType, Field};

    fn make_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("window_id", DataType::Int64, false),
            Field::new("key_hash", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    fn make_batch(schema: Arc<Schema>, window_id: i64, key_hash: i64, value: f64) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![window_id])),
                Arc::new(Int64Array::from(vec![key_hash])),
                Arc::new(Float64Array::from(vec![value])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_new_empty() {
        let schema = make_schema();
        let results = WindowResults::new(schema);
        assert!(results.is_empty());
        assert_eq!(results.total_rows(), 0);
        assert_eq!(results.total_rows_including_late(), 0);
        assert!(results.side_output.is_empty());
    }

    #[test]
    fn test_push_batch_total_rows() {
        let schema = make_schema();
        let mut results = WindowResults::new(schema.clone());
        results.batches.push(make_batch(schema.clone(), 0, 0, 10.0));
        results.batches.push(make_batch(schema.clone(), 1, 0, 20.0));
        assert_eq!(results.total_rows(), 2);
        assert!(!results.is_empty());
    }

    #[test]
    fn test_late_batches_count_in_total_including_late() {
        let schema = make_schema();
        let mut results = WindowResults::new(schema.clone());
        results.batches.push(make_batch(schema.clone(), 0, 0, 10.0));
        results
            .late_batches
            .push(make_batch(schema.clone(), 0, 0, 5.0));
        assert_eq!(results.total_rows(), 1);
        assert_eq!(results.total_rows_including_late(), 2);
    }

    #[test]
    fn test_side_output_push_and_count() {
        let schema = make_schema();
        let results_schema = make_schema();
        let mut results = WindowResults::new(results_schema);
        results
            .side_output
            .push(make_batch(schema.clone(), 0, 0, 99.0));
        assert_eq!(results.side_output.total_rows(), 1);
        assert!(!results.side_output.is_empty());
    }

    #[test]
    fn test_schema_accessible() {
        let schema = make_schema();
        let results = WindowResults::new(schema.clone());
        assert_eq!(results.schema.fields().len(), 3);
        assert_eq!(results.schema.field(0).name(), "window_id");
    }

    #[test]
    fn test_side_output_default() {
        let so: SideOutput<DroppedLate> = SideOutput::default();
        assert!(so.is_empty());
        assert_eq!(so.total_rows(), 0);
    }
}
