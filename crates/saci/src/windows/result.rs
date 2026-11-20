//! Window aggregation results container.
//!
//! [`WindowResults`] is an ephemeral container stored as a dataset resource
//! after a [`WindowedSystem`](super::WindowedSystem) runs. Downstream systems
//! consume it; it is not persisted via IPC.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;

/// Ephemeral container for windowed aggregation output.
///
/// Produced by [`WindowedSystem`](super::WindowedSystem) and inserted
/// into the dataset as a resource via
/// [`Dataset::insert_resource`](crate::dataset::Dataset::insert_resource).
///
/// Each run replaces the previous `WindowResults` with the **finalized windows
/// for that run only**. Cross-run accumulation belongs to the
/// [`WindowAccumulator`](super::WindowAccumulator) component, which
/// the system updates and the distributed runner checkpoints.
#[derive(Debug)]
pub struct WindowResults {
    /// Schema shared by all result batches.
    ///
    /// For a `Reduce` aggregate this holds `window_id` (Int64), `key_hash`
    /// (Int64) and the aggregated value column, named after the aggregate
    /// and the field (`sum_price`); a session window adds
    /// `session_start_ts` and `session_end_ts`. An empty run publishes the
    /// same schema as a non-empty one.
    pub schema: Arc<Schema>,

    /// Result batches of on-time window firings, one row per aggregated
    /// group. All batches share `schema`.
    pub on_time: Vec<RecordBatch>,

    /// Result batches of late re-firings within the allowed-lateness window,
    /// one row per group. All batches share `schema`.
    pub late: Vec<RecordBatch>,

    /// Source rows dropped because their timestamp fell beyond the
    /// allowed-lateness budget, in the source component's schema.
    pub dropped: Vec<RecordBatch>,
}

impl WindowResults {
    /// Create a new `WindowResults` with the given schema and no batches.
    pub fn new(schema: Arc<Schema>) -> Self {
        Self {
            schema,
            on_time: Vec::new(),
            late: Vec::new(),
            dropped: Vec::new(),
        }
    }

    /// Total number of on-time output rows across all result batches.
    pub fn total_rows(&self) -> usize {
        self.on_time.iter().map(|b| b.num_rows()).sum()
    }

    /// Total rows including both on-time and late re-firings.
    pub fn total_rows_including_late(&self) -> usize {
        self.total_rows() + self.late.iter().map(|b| b.num_rows()).sum::<usize>()
    }

    /// `true` when no on-time result batches have been produced.
    pub fn is_empty(&self) -> bool {
        self.on_time.is_empty()
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
        let results = WindowResults::new(make_schema());
        assert!(results.is_empty());
        assert_eq!(results.total_rows(), 0);
        assert_eq!(results.total_rows_including_late(), 0);
        assert!(results.dropped.is_empty());
    }

    #[test]
    fn test_push_batch_total_rows() {
        let schema = make_schema();
        let mut results = WindowResults::new(schema.clone());
        results.on_time.push(make_batch(schema.clone(), 0, 0, 10.0));
        results.on_time.push(make_batch(schema.clone(), 1, 0, 20.0));
        assert_eq!(results.total_rows(), 2);
        assert!(!results.is_empty());
    }

    #[test]
    fn test_late_batches_count_in_total_including_late() {
        let schema = make_schema();
        let mut results = WindowResults::new(schema.clone());
        results.on_time.push(make_batch(schema.clone(), 0, 0, 10.0));
        results.late.push(make_batch(schema.clone(), 0, 0, 5.0));
        assert_eq!(results.total_rows(), 1);
        assert_eq!(results.total_rows_including_late(), 2);
    }

    #[test]
    fn test_schema_accessible() {
        let results = WindowResults::new(make_schema());
        assert_eq!(results.schema.fields().len(), 3);
        assert_eq!(results.schema.field(0).name(), "window_id");
    }
}
