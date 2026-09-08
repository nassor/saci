//! [`WindowAccumulator`]: durable per-window aggregate state stored as a [`Component`].
//!
//! Each row holds the running aggregate for one
//! `(source_component, window_id, key_hash)` triple. Every numeric field is
//! nullable so later schema versions can add columns without breaking existing
//! checkpoint payloads.
//!
//! ## Schema versioning
//!
//! The `version` field is the compatibility discriminator:
//!
//! | Version | Meaning |
//! |---------|---------|
//! | `None`  | Treated as v1 by [`migrate_to_current`]. |
//! | `Some(1)` | Current version. All accumulator fields present. |
//! | `Some(n > 1)` | Newer binary. Rejected with `SaciError::configuration`. |
//!
//! Adding a field means bumping [`CURRENT_ACCUMULATOR_VERSION`], writing
//! `migrate_v{n}_to_v{n+1}(batch) -> SaciResult<RecordBatch>`, and calling it from
//! [`migrate_to_current`] under the matching version arm.

use std::sync::Arc;

use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

use crate::SaciError;
use crate::SaciResult;
use crate::component::Component;
#[cfg(feature = "distributed")]
use crate::dataset::Dataset;

/// The current schema version written by this binary.
pub const CURRENT_ACCUMULATOR_VERSION: u32 = 1;

/// Persistent per-window aggregate state for one `(source_component, window_id, key_hash)` group.
///
/// A [`WindowedSystem`](super::system::WindowedSystem) appends rows at the end of
/// each run and reads them back at the start of the next to continue accumulation
/// across distributed batch claims.
///
/// Numeric fields are `Option<…>` so columns added in later schema versions read
/// as Arrow nulls in older checkpoints. `version` gates dispatch in
/// [`migrate_to_current`].
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct WindowAccumulator {
    /// Schema version written by the producing binary. `None` is treated as v1.
    pub version: Option<u32>,
    /// Name of the source component this accumulator belongs to.
    /// Disambiguates rows when multiple `WindowedSystem`s run on the same pipeline.
    pub source_component: String,
    /// Tumbling/sliding window ID or session ID.
    pub window_id: i64,
    /// Per-row key hash (`0` for global / non-keyed windows).
    pub key_hash: i64,
    /// Row count accumulated so far in this window group.
    pub count: i64,
    /// Running sum (nullable; `None` when the aggregate type does not use sum).
    pub sum_f64: Option<f64>,
    /// Running minimum (nullable).
    pub min_f64: Option<f64>,
    /// Running maximum (nullable).
    pub max_f64: Option<f64>,
    /// Session start timestamp in milliseconds (nullable; only set for session windows).
    pub session_start_ts: Option<i64>,
    /// Session end timestamp in milliseconds (nullable; only set for session windows).
    pub session_end_ts: Option<i64>,
    /// Watermark at which this window was considered finalized.
    /// `None` until a watermark is wired up.
    pub finalized_at_watermark: Option<i64>,
}

impl Component for WindowAccumulator {
    fn name() -> &'static str {
        "WindowAccumulator"
    }

    fn version() -> u32 {
        CURRENT_ACCUMULATOR_VERSION
    }

    fn migrate(from_version: u32, batch: RecordBatch) -> crate::SaciResult<RecordBatch> {
        migrate_to_current_inner(from_version, batch)
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("version", DataType::UInt32, true),
            Field::new("source_component", DataType::Utf8, false),
            Field::new("window_id", DataType::Int64, false),
            Field::new("key_hash", DataType::Int64, false),
            Field::new("count", DataType::Int64, false),
            Field::new("sum_f64", DataType::Float64, true),
            Field::new("min_f64", DataType::Float64, true),
            Field::new("max_f64", DataType::Float64, true),
            Field::new("session_start_ts", DataType::Int64, true),
            Field::new("session_end_ts", DataType::Int64, true),
            Field::new("finalized_at_watermark", DataType::Int64, true),
        ]))
    }
}

/// Migrate a `RecordBatch` decoded from a checkpoint to the current schema version.
///
/// Reads the `version` column, treating its absence as v1, and applies any
/// backfill migrations in order. Returns the batch unchanged when it is already
/// at [`CURRENT_ACCUMULATOR_VERSION`].
///
/// # Errors
///
/// Returns `SaciError::configuration` if the batch was produced by a newer
/// binary (version > [`CURRENT_ACCUMULATOR_VERSION`]).
pub fn migrate_to_current(batch: RecordBatch) -> SaciResult<RecordBatch> {
    use arrow_array::UInt32Array;

    let detected_version = if let Ok(idx) = batch.schema().index_of("version") {
        let col = batch.column(idx);
        if let Some(arr) = col.as_any().downcast_ref::<UInt32Array>() {
            // First non-null value wins.
            (0..arr.len()).find_map(|i| {
                if arr.is_valid(i) {
                    Some(arr.value(i))
                } else {
                    None
                }
            })
        } else {
            None
        }
    } else {
        None
    };

    migrate_to_current_inner(detected_version.unwrap_or(1), batch)
}

/// Inner migration dispatcher used by both [`migrate_to_current`] and
/// [`Component::migrate`](crate::component::Component::migrate).
fn migrate_to_current_inner(from_version: u32, batch: RecordBatch) -> SaciResult<RecordBatch> {
    if from_version > CURRENT_ACCUMULATOR_VERSION {
        return Err(SaciError::configuration(format!(
            "WindowAccumulator checkpoint was written by a newer binary (version={from_version}); \
             upgrade saci to read this checkpoint"
        )));
    }

    // v1 needs no migration.
    Ok(batch)
}

#[cfg(feature = "distributed")]
impl super::system::WindowedSystem {
    /// Update the pipeline's `WindowAccumulator` component with fresh aggregate results.
    ///
    /// For each result batch, the existing accumulator row matching
    /// `source_component`, `window_id`, and `key_hash` is marked dead, the new row
    /// is appended, and a compaction at the end drops the dead rows.
    pub(super) fn flush_accumulator(
        &self,
        pipeline: &mut Dataset,
        result_batches: &[RecordBatch],
    ) -> Result<(), SaciError> {
        use arrow_array::{Int64Array, StringArray};

        if result_batches.is_empty() {
            return Ok(());
        }

        // Groups this call is about to write.
        let mut new_groups: std::collections::HashSet<(i64, i64)> =
            std::collections::HashSet::new();
        for rb in result_batches {
            if rb.num_rows() == 0 {
                continue;
            }
            let wid_idx = rb.schema().index_of("window_id").ok();
            let kh_idx = rb.schema().index_of("key_hash").ok();
            if let (Some(wi), Some(ki)) = (wid_idx, kh_idx) {
                let wid_col = rb.column(wi).as_any().downcast_ref::<Int64Array>();
                let kh_col = rb.column(ki).as_any().downcast_ref::<Int64Array>();
                if let (Some(wc), Some(kc)) = (wid_col, kh_col) {
                    for r in 0..rb.num_rows() {
                        new_groups.insert((wc.value(r), kc.value(r)));
                    }
                }
            }
        }

        // Mark superseded accumulator rows as dead.
        if let Some(acc_batch) = pipeline.batch_for(WindowAccumulator::name()) {
            let acc_batch = acc_batch.clone();
            let src_idx = acc_batch.schema().index_of("source_component").ok();
            let wid_idx = acc_batch.schema().index_of("window_id").ok();
            let kh_idx = acc_batch.schema().index_of("key_hash").ok();

            if let (Some(si), Some(wi), Some(ki)) = (src_idx, wid_idx, kh_idx) {
                let src_col = acc_batch.column(si).as_any().downcast_ref::<StringArray>();
                let wid_col = acc_batch.column(wi).as_any().downcast_ref::<Int64Array>();
                let kh_col = acc_batch.column(ki).as_any().downcast_ref::<Int64Array>();

                if let (Some(sc), Some(wc), Some(kc)) = (src_col, wid_col, kh_col) {
                    let row_range = pipeline.row_range();
                    for (row_offset, abs_row) in row_range.enumerate() {
                        if row_offset >= acc_batch.num_rows() {
                            break;
                        }
                        let matches_component = sc.value(row_offset) == self.source_component;
                        let group = (wc.value(row_offset), kc.value(row_offset));
                        if matches_component && new_groups.contains(&group) {
                            pipeline.mark_dead(crate::row::Row::new(abs_row));
                        }
                    }
                }
            }
        }

        // Build new accumulator rows from the aggregation results.
        let new_rows: Vec<WindowAccumulator> = result_batches
            .iter()
            .filter_map(|rb| {
                if rb.num_rows() == 0 {
                    return None;
                }
                let wid_idx = rb.schema().index_of("window_id").ok()?;
                let kh_idx = rb.schema().index_of("key_hash").ok()?;
                let wid_col = rb.column(wid_idx).as_any().downcast_ref::<Int64Array>()?;
                let kh_col = rb.column(kh_idx).as_any().downcast_ref::<Int64Array>()?;
                let sts_idx = rb.schema().index_of("session_start_ts").ok();
                let ste_idx = rb.schema().index_of("session_end_ts").ok();
                let sts_val = sts_idx.and_then(|i| {
                    rb.column(i)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .and_then(|a| {
                            if a.is_valid(0) {
                                Some(a.value(0))
                            } else {
                                None
                            }
                        })
                });
                let ste_val = ste_idx.and_then(|i| {
                    rb.column(i)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .and_then(|a| {
                            if a.is_valid(0) {
                                Some(a.value(0))
                            } else {
                                None
                            }
                        })
                });

                // Aggregate value: any Float64 column other than the id columns.
                let mut sum_f64 = None;
                let mut count = 0i64;
                let rb_schema = rb.schema();
                for col_idx in 0..rb_schema.fields().len() {
                    let field = rb_schema.field(col_idx);
                    if field.name() == "window_id"
                        || field.name() == "key_hash"
                        || field.name() == "session_start_ts"
                        || field.name() == "session_end_ts"
                    {
                        continue;
                    }
                    if let arrow_schema::DataType::Float64 = field.data_type()
                        && let Some(arr) = rb
                            .column(col_idx)
                            .as_any()
                            .downcast_ref::<arrow_array::Float64Array>()
                    {
                        if arr.is_valid(0) {
                            sum_f64 = Some(arr.value(0));
                        }
                        count = 1;
                    }
                    if let arrow_schema::DataType::Int64 = field.data_type()
                        && let Some(arr) = rb.column(col_idx).as_any().downcast_ref::<Int64Array>()
                        && arr.is_valid(0)
                    {
                        count = arr.value(0);
                    }
                }

                Some(WindowAccumulator {
                    version: Some(CURRENT_ACCUMULATOR_VERSION),
                    source_component: self.source_component.to_string(),
                    window_id: wid_col.value(0),
                    key_hash: kh_col.value(0),
                    count,
                    sum_f64,
                    min_f64: None,
                    max_f64: None,
                    session_start_ts: sts_val,
                    session_end_ts: ste_val,
                    finalized_at_watermark: None,
                })
            })
            .collect();

        if !new_rows.is_empty() {
            pipeline
                .append::<WindowAccumulator>(&new_rows)
                .map_err(|e| {
                    SaciError::generic(format!("WindowedSystem: accumulator append error: {e}"))
                })?;
        }

        // Drop the rows superseded above.
        pipeline
            .compact()
            .map_err(|e| SaciError::generic(format!("WindowedSystem: compact error: {e}")))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, StringArray, UInt32Array};

    fn make_accumulator(
        version: Option<u32>,
        source: &str,
        wid: i64,
        kh: i64,
    ) -> WindowAccumulator {
        WindowAccumulator {
            version,
            source_component: source.to_string(),
            window_id: wid,
            key_hash: kh,
            count: 3,
            sum_f64: Some(10.5),
            min_f64: Some(1.0),
            max_f64: Some(9.5),
            session_start_ts: None,
            session_end_ts: None,
            finalized_at_watermark: None,
        }
    }

    #[test]
    fn test_schema_field_count() {
        let schema = WindowAccumulator::schema();
        assert_eq!(schema.fields().len(), 11);
    }

    #[test]
    fn test_schema_nullable_fields() {
        let schema = WindowAccumulator::schema();
        assert!(schema.field_with_name("version").unwrap().is_nullable());
        assert!(
            !schema
                .field_with_name("source_component")
                .unwrap()
                .is_nullable()
        );
        for field_name in &[
            "sum_f64",
            "min_f64",
            "max_f64",
            "session_start_ts",
            "session_end_ts",
            "finalized_at_watermark",
        ] {
            assert!(
                schema.field_with_name(field_name).unwrap().is_nullable(),
                "{field_name} should be nullable"
            );
        }
    }

    #[test]
    fn test_round_trip_serde_arrow() {
        let rows = vec![
            make_accumulator(Some(1), "Trade", 0, 100),
            make_accumulator(Some(1), "Trade", 1, 200),
        ];

        let batch = WindowAccumulator::to_record_batch(&rows).expect("serialization failed");
        assert_eq!(batch.num_rows(), 2);

        let recovered =
            WindowAccumulator::from_record_batch(&batch).expect("deserialization failed");
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].window_id, 0);
        assert_eq!(recovered[1].key_hash, 200);
        assert_eq!(recovered[0].sum_f64, Some(10.5));
    }

    #[test]
    fn test_round_trip_nullable_fields() {
        let rows = vec![WindowAccumulator {
            version: Some(1),
            source_component: "Orders".to_string(),
            window_id: 42,
            key_hash: 0,
            count: 1,
            sum_f64: None,
            min_f64: None,
            max_f64: None,
            session_start_ts: Some(1_700_000_000_000),
            session_end_ts: Some(1_700_000_030_000),
            finalized_at_watermark: None,
        }];

        let batch = WindowAccumulator::to_record_batch(&rows).unwrap();
        let recovered = WindowAccumulator::from_record_batch(&batch).unwrap();
        assert_eq!(recovered[0].sum_f64, None);
        assert_eq!(recovered[0].session_start_ts, Some(1_700_000_000_000));
        assert_eq!(recovered[0].session_end_ts, Some(1_700_000_030_000));
    }

    #[test]
    fn test_name() {
        assert_eq!(WindowAccumulator::name(), "WindowAccumulator");
    }

    #[test]
    fn test_migrate_v1_is_identity() {
        let rows = vec![make_accumulator(Some(1), "A", 0, 0)];
        let batch = WindowAccumulator::to_record_batch(&rows).unwrap();
        let migrated = migrate_to_current(batch.clone()).unwrap();
        assert_eq!(migrated.num_rows(), 1);
    }

    #[test]
    fn test_migrate_none_version_treated_as_v1() {
        let rows = vec![make_accumulator(None, "B", 5, 7)];
        let batch = WindowAccumulator::to_record_batch(&rows).unwrap();
        let migrated = migrate_to_current(batch).unwrap();
        assert_eq!(migrated.num_rows(), 1);
    }

    #[test]
    fn test_migrate_future_version_rejected() {
        // Build a batch with version = 999 directly using Arrow arrays.
        let schema = WindowAccumulator::schema();
        let n: usize = 1;

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![999u32])) as _,
                Arc::new(StringArray::from(vec!["X"])) as _,
                Arc::new(Int64Array::from(vec![0i64])) as _,
                Arc::new(Int64Array::from(vec![0i64])) as _,
                Arc::new(Int64Array::from(vec![0i64])) as _,
                Arc::new(Float64Array::from(vec![None::<f64>])) as _,
                Arc::new(Float64Array::from(vec![None::<f64>])) as _,
                Arc::new(Float64Array::from(vec![None::<f64>])) as _,
                Arc::new(Int64Array::from(vec![None::<i64>])) as _,
                Arc::new(Int64Array::from(vec![None::<i64>])) as _,
                Arc::new(Int64Array::from(vec![None::<i64>])) as _,
            ],
        )
        .unwrap();
        let _ = n; // suppress unused warning

        let result = migrate_to_current(batch);
        assert!(result.is_err(), "expected config error for future version");
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("newer binary"), "error message: {msg}");
    }

    #[test]
    fn test_nullable_preservation_in_batch() {
        // Verify that Option<f64> = None round-trips as a null in the Arrow batch.
        let rows = vec![make_accumulator(Some(1), "T", 0, 0)];
        let batch = WindowAccumulator::to_record_batch(&rows).unwrap();
        let idx = batch.schema().index_of("session_start_ts").unwrap();
        let col = batch.column(idx);
        assert!(!col.is_valid(0), "expected null for None Option");
    }

    #[test]
    fn test_boolean_alive_column_not_present() {
        // WindowAccumulator schema should NOT have a boolean "alive" field.
        let schema = WindowAccumulator::schema();
        let has_bool = schema
            .fields()
            .iter()
            .any(|f| *f.data_type() == DataType::Boolean);
        assert!(!has_bool);
    }
}
