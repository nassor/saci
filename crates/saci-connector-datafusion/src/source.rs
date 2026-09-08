//! [`DataFusionSource`]: a [`Source`] backed by a DataFusion SQL query.
//!
//! Executes a SQL statement against a `SessionContext` and streams the
//! resulting [`RecordBatch`]es into a SACI pipeline.
//!
//! ```rust
//! use std::sync::Arc;
//! use arrow_schema::{DataType, Field, Schema};
//! use saci_connector_datafusion::DataFusionSource;
//! use saci_core::io::source::Source;
//! use datafusion::prelude::SessionContext;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let ctx = SessionContext::new();
//! // Register tables and run SQL …
//! // let mut src = DataFusionSource::from_sql(&ctx, "SELECT 1 AS n").await.unwrap();
//! # }
//! ```

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use tokio::sync::Mutex;

use saci_core::error::SaciError;
use saci_core::io::source::Source;

/// A [`Source`] that streams [`RecordBatch`]es from a DataFusion SQL query.
///
/// Construct with [`DataFusionSource::from_sql`], or
/// [`DataFusionSource::from_stream`] when a [`SendableRecordBatchStream`] is
/// already at hand. The stream sits behind a [`Mutex`] so the struct is `Sync`,
/// as the `#[async_trait]` bounds on `Source` require.
///
/// Batches are pulled lazily: DataFusion executes the query on the executor
/// partition holding the stream. `estimated_rows` is `None` unless set via
/// [`DataFusionSource::with_estimated_rows`].
pub struct DataFusionSource {
    stream: Mutex<SendableRecordBatchStream>,
    schema: Arc<Schema>,
    estimated_rows: Option<usize>,
}

impl DataFusionSource {
    /// Execute a SQL query against a DataFusion `SessionContext` and wrap the
    /// resulting batch stream as a [`Source`].
    ///
    /// # Errors
    ///
    /// Returns `SaciError::Generic` if DataFusion fails to parse or plan the
    /// SQL, or if the execution stream cannot be obtained.
    ///
    /// # Example
    ///
    /// ```rust
    /// use std::sync::Arc;
    /// use saci_connector_datafusion::DataFusionSource;
    /// use datafusion::prelude::SessionContext;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let ctx = SessionContext::new();
    /// // let src = DataFusionSource::from_sql(&ctx, "SELECT 1 AS n").await.unwrap();
    /// # }
    /// ```
    pub async fn from_sql(
        ctx: &datafusion::prelude::SessionContext,
        sql: &str,
    ) -> Result<Self, SaciError> {
        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| SaciError::generic(format!("DataFusionSource: SQL error: {e}")))?;
        let schema = Arc::new(df.schema().as_arrow().clone());
        let stream = df.execute_stream().await.map_err(|e| {
            SaciError::generic(format!("DataFusionSource: execute_stream error: {e}"))
        })?;
        Ok(Self {
            stream: Mutex::new(stream),
            schema,
            estimated_rows: None,
        })
    }

    /// Wrap an existing [`SendableRecordBatchStream`] as a [`Source`].
    ///
    /// Useful when the stream already exists, for example from a DataFusion
    /// Parquet scan or a custom execution plan.
    pub fn from_stream(stream: SendableRecordBatchStream) -> Self {
        let schema = stream.schema();
        Self {
            stream: Mutex::new(stream),
            schema,
            estimated_rows: None,
        }
    }

    /// Set an estimated row count for progress reporting.
    ///
    /// DataFusion does not always expose row counts before execution, so this
    /// is an optional hint.
    pub fn with_estimated_rows(mut self, rows: usize) -> Self {
        self.estimated_rows = Some(rows);
        self
    }
}

#[async_trait]
impl Source for DataFusionSource {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        let mut stream = self.stream.lock().await;
        match stream.next().await {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => Err(SaciError::generic(format!(
                "DataFusionSource: stream error: {e}"
            ))),
            None => Ok(None),
        }
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.estimated_rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use saci_core::dataset::Dataset;
    use saci_core::io::source::drain_into_dataset;

    fn make_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn make_batch(schema: Arc<Schema>, ids: &[i32], names: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    fn build_ctx_with_table(batch: RecordBatch) -> SessionContext {
        let ctx = SessionContext::new();
        let schema = batch.schema();
        let provider =
            MemTable::try_new(schema, vec![vec![batch]]).expect("MemTable creation failed");
        ctx.register_table("test_table", Arc::new(provider))
            .expect("register_table failed");
        ctx
    }

    #[tokio::test]
    async fn test_sql_query_against_in_memory_table() {
        let schema = make_schema();
        let batch = make_batch(schema.clone(), &[1, 2, 3], &["alice", "bob", "carol"]);
        let ctx = build_ctx_with_table(batch);

        let mut src = DataFusionSource::from_sql(&ctx, "SELECT id, name FROM test_table")
            .await
            .unwrap();

        assert_eq!(src.schema().fields().len(), 2);
        assert_eq!(src.schema().field(0).name(), "id");
        assert_eq!(src.schema().field(1).name(), "name");

        let mut total_rows = 0;
        while let Some(batch) = src.next_batch().await.unwrap() {
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_filter_and_projection() {
        let schema = make_schema();
        let batch = make_batch(schema.clone(), &[1, 2, 3, 4, 5], &["a", "b", "c", "d", "e"]);
        let ctx = build_ctx_with_table(batch);

        let mut src = DataFusionSource::from_sql(&ctx, "SELECT id FROM test_table WHERE id > 2")
            .await
            .unwrap();

        assert_eq!(src.schema().fields().len(), 1);
        assert_eq!(src.schema().field(0).name(), "id");

        // ids 3, 4, 5 pass the filter.
        let mut total_rows = 0;
        let mut ids_seen: Vec<i32> = Vec::new();
        while let Some(batch) = src.next_batch().await.unwrap() {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for i in 0..col.len() {
                ids_seen.push(col.value(i));
            }
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 3);
        for id in &ids_seen {
            assert!(*id > 2, "id {id} should be > 2");
        }
    }

    #[tokio::test]
    async fn test_drain_into_pipeline() {
        let schema = make_schema();
        let batch = make_batch(schema.clone(), &[10, 20, 30, 40], &["w", "x", "y", "z"]);
        let ctx = build_ctx_with_table(batch);

        let mut src = DataFusionSource::from_sql(&ctx, "SELECT id, name FROM test_table")
            .await
            .unwrap();

        let src_schema = src.schema();
        let mut dataset = Dataset::new();
        dataset.register_raw_component("records", src_schema);

        let rows: usize = drain_into_dataset(&mut src, &mut dataset, "records")
            .await
            .unwrap();

        assert_eq!(rows, 4);
        assert_eq!(dataset.rows(), 4);
    }

    #[tokio::test]
    async fn test_empty_result_returns_none() {
        let schema = make_schema();
        let batch = make_batch(schema.clone(), &[1, 2], &["a", "b"]);
        let ctx = build_ctx_with_table(batch);

        let mut src = DataFusionSource::from_sql(&ctx, "SELECT id FROM test_table WHERE id > 9999")
            .await
            .unwrap();

        let first = src.next_batch().await.unwrap();
        assert!(
            first.is_none(),
            "expected None for empty result set, got Some"
        );
    }

    #[tokio::test]
    async fn test_invalid_sql_returns_error() {
        let ctx = SessionContext::new();
        let result = DataFusionSource::from_sql(&ctx, "SELECT FROM WHERE totally invalid;;;").await;
        assert!(result.is_err(), "expected error for invalid SQL");
        let err = result.err().unwrap();
        let msg = err.message();
        assert!(!msg.is_empty(), "error message should be non-empty");
    }

    #[tokio::test]
    async fn test_from_stream_wraps_correctly() {
        let schema = make_schema();
        let batch = make_batch(schema.clone(), &[100, 200], &["foo", "bar"]);
        let ctx = build_ctx_with_table(batch);

        let df = ctx.sql("SELECT id, name FROM test_table").await.unwrap();
        let stream = df.execute_stream().await.unwrap();

        let mut src = DataFusionSource::from_stream(stream);
        assert_eq!(src.schema().fields().len(), 2);

        let mut total = 0;
        while let Some(batch) = src.next_batch().await.unwrap() {
            total += batch.num_rows();
        }
        assert_eq!(total, 2);
    }

    #[tokio::test]
    async fn test_estimated_rows_propagates() {
        let schema = make_schema();
        let batch = make_batch(schema.clone(), &[1], &["x"]);
        let ctx = build_ctx_with_table(batch);

        let src = DataFusionSource::from_sql(&ctx, "SELECT id FROM test_table")
            .await
            .unwrap()
            .with_estimated_rows(42);

        assert_eq!(src.estimated_rows(), Some(42));
    }
}
