//! [`ChannelSource`]: in-memory [`Source`] over a
//! [`tokio::sync::mpsc::Receiver<RecordBatch>`]. Dropping the sender signals EOF,
//! and a batch that does not match the schema declared at construction is an
//! error.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use tokio::sync::mpsc;

use saci_core::error::SaciError;
use saci_core::io::source::Source;

/// In-memory pull source backed by a tokio mpsc channel.
///
/// # Example
///
/// ```rust
/// use std::sync::Arc;
/// use arrow_schema::{DataType, Field, Schema};
/// use saci_connector_channel::ChannelSource;
/// use saci_core::io::source::Source;
///
/// # #[tokio::main]
/// # async fn main() {
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("x", DataType::Float32, false),
/// ]));
/// let (tx, mut src) = ChannelSource::new(schema.clone(), 4);
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
    pub fn new(schema: Arc<Schema>, buffer: usize) -> (mpsc::Sender<RecordBatch>, Self) {
        let (tx, rx) = mpsc::channel(buffer);
        (tx, Self { rx, schema })
    }

    /// Wrap an existing receiver half, paired with a sender resolved
    /// elsewhere — the [`ChannelRegistry`](crate::registry::ChannelRegistry)
    /// bridge's source-side constructor.
    pub fn from_receiver(schema: Arc<Schema>, rx: mpsc::Receiver<RecordBatch>) -> Self {
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
                if batch.schema_ref().fields() != self.schema.fields() {
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
        let (tx, mut src) = ChannelSource::new(make_schema(), 4);
        drop(tx);
        assert!(src.next_batch().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_channel_source_yields_batches_in_order() {
        let schema = make_schema();
        let (tx, mut src) = ChannelSource::new(schema.clone(), 8);

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
        let (tx, mut src) = ChannelSource::new(schema.clone(), 4);

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

    #[tokio::test]
    async fn test_channel_source_schema_accessor() {
        let schema = make_schema();
        let (_tx, src) = ChannelSource::new(schema.clone(), 1);
        assert_eq!(src.schema().fields().len(), 1);
        assert_eq!(src.schema().field(0).name(), "v");
    }
}
