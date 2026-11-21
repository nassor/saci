//! [`ChannelSource`]: in-memory [`Source`] over a
//! [`tokio::sync::mpsc::Receiver<RecordBatch>`]. Dropping the sender signals EOF,
//! and a batch that does not match the schema declared at construction is an
//! error.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::error::SaciError;
use crate::io::Source;

/// In-memory pull source backed by a tokio mpsc channel.
///
/// # Example
///
/// ```rust
/// use std::sync::Arc;
/// use arrow_schema::{DataType, Field, Schema};
/// use saci::connector::channel::ChannelSource;
/// use saci::io::Source;
///
/// # #[tokio::main]
/// # async fn main() {
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("x", DataType::Float32, false),
/// ]));
/// let (mut src, tx) = ChannelSource::new(schema.clone(), 4);
/// drop(tx); // signal EOF
/// assert!(src.next_batch().await.unwrap().is_none());
/// # }
/// ```
pub struct ChannelSource {
    rx: mpsc::Receiver<RecordBatch>,
    schema: Arc<Schema>,
}

impl ChannelSource {
    /// Create a `ChannelSource` and the matching `Sender`.
    ///
    /// `buffer` is the mpsc channel capacity: batches that can queue without
    /// blocking the sender. Drop the sender to signal EOF.
    pub fn new(schema: Arc<Schema>, buffer: usize) -> (Self, mpsc::Sender<RecordBatch>) {
        let (tx, rx) = mpsc::channel(buffer);
        (Self { rx, schema }, tx)
    }

    /// Wrap an existing receiver half, paired with a sender resolved
    /// elsewhere — the [`ChannelRegistry`](crate::connector::channel::registry::ChannelRegistry)
    /// bridge's source-side constructor.
    pub fn with_receiver(schema: Arc<Schema>, rx: mpsc::Receiver<RecordBatch>) -> Self {
        Self { rx, schema }
    }
}

#[async_trait]
impl Source for ChannelSource {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        match self.rx.recv().await {
            None => Ok(None),
            Some(batch) => {
                if !Arc::ptr_eq(batch.schema_ref(), &self.schema)
                    && batch.schema_ref().fields() != self.schema.fields()
                {
                    return Err(SaciError::generic(format!(
                        "ChannelSource: received batch with schema {:?}, expected {:?}",
                        batch.schema_ref(),
                        self.schema
                    )));
                }
                Ok(Some(batch))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};

    fn make_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]))
    }

    fn make_batch(schema: Arc<Schema>, values: &[i32]) -> RecordBatch {
        let arr = Arc::new(Int32Array::from(values.to_vec()));
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    #[tokio::test]
    async fn test_channel_source_eof_on_sender_drop() {
        let (mut src, tx) = ChannelSource::new(make_schema(), 4);
        drop(tx);
        assert!(src.next_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_channel_source_yields_batches_in_order() {
        let schema = make_schema();
        let (mut src, tx) = ChannelSource::new(schema.clone(), 8);

        for i in 0i32..5 {
            tx.send(make_batch(schema.clone(), &[i])).await.unwrap();
        }
        drop(tx);

        let mut count = 0i32;
        while let Some(batch) = src.next_batch().await.unwrap() {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(col.value(0), count);
            count += 1;
        }
        assert_eq!(count, 5);
    }

    #[tokio::test]
    async fn test_channel_source_schema_mismatch_returns_error() {
        let schema = make_schema();
        let wrong_schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, false)]));
        let (mut src, tx) = ChannelSource::new(schema.clone(), 4);

        let arr: Arc<dyn arrow_array::Array> =
            Arc::new(arrow_array::Float64Array::from(vec![1.0f64]));
        let wrong_batch = RecordBatch::try_new(wrong_schema, vec![arr]).unwrap();
        tx.send(wrong_batch).await.unwrap();
        drop(tx);

        let result = src.next_batch().await;
        assert!(result.is_err());
        let msg = result.unwrap_err().message();
        assert!(msg.contains("ChannelSource"));
    }

    /// The schema `Arc` is compared first as a shortcut, so a batch built on
    /// its own equal copy of the schema must still pass the field comparison
    /// rather than be refused for not being the same allocation.
    #[tokio::test]
    async fn test_channel_source_accepts_an_equal_schema_in_a_distinct_arc() {
        let (mut src, tx) = ChannelSource::new(make_schema(), 4);
        let copy = make_schema();
        assert!(!Arc::ptr_eq(&copy, &src.schema()));

        tx.send(make_batch(copy, &[7])).await.unwrap();
        drop(tx);

        let batch = src.next_batch().await.unwrap().expect("one batch");
        assert_eq!(batch.num_rows(), 1);
    }

    #[tokio::test]
    async fn test_channel_source_schema_accessor() {
        let schema = make_schema();
        let (src, _tx) = ChannelSource::new(schema.clone(), 1);
        assert_eq!(src.schema().fields().len(), 1);
        assert_eq!(src.schema().field(0).name(), "v");
    }
}
