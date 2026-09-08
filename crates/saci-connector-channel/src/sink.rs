//! [`ChannelSink`]: in-memory [`Sink`] that sends each [`RecordBatch`] through a
//! tokio mpsc channel. The receiver collects results without file I/O.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use tokio::sync::mpsc;

use saci_core::error::SaciError;
use saci_core::io::sink::Sink;

/// In-memory push sink backed by a tokio mpsc channel.
///
/// # Example
///
/// ```rust
/// use std::sync::Arc;
/// use arrow_schema::{DataType, Field, Schema};
/// use saci_connector_channel::ChannelSink;
/// use saci_core::io::sink::Sink;
///
/// # #[tokio::main]
/// # async fn main() {
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("x", DataType::Float32, false),
/// ]));
/// let (mut sink, _rx) = ChannelSink::new(schema.clone(), 4);
/// sink.finish().await.unwrap();
/// # }
/// ```
pub struct ChannelSink {
    tx: mpsc::Sender<RecordBatch>,
    schema: Arc<Schema>,
    buffer_capacity: usize,
    /// Cumulative rows sent *before* each message that may still be queued,
    /// oldest first.
    ///
    /// A channel queues whole batches, so the number of messages in flight is
    /// not the row backlog [`Sink::pending_rows`] is defined to report. The
    /// channel is FIFO and a `ChannelSink` owns the only sender, so the `k`
    /// messages still queued are always the last `k` pushed here: the backlog
    /// is `sent_rows` minus the entry `k` from the back, which is an O(1)
    /// index into a `VecDeque`. Trimmed on every write, so it never grows past
    /// `buffer_capacity` entries.
    queued: VecDeque<u64>,
    /// Total rows handed to the channel since construction.
    sent_rows: u64,
}

impl ChannelSink {
    /// Create a `ChannelSink` and the matching `Receiver`.
    ///
    /// `buffer` is the mpsc channel capacity.
    pub fn new(schema: Arc<Schema>, buffer: usize) -> (Self, mpsc::Receiver<RecordBatch>) {
        let (tx, rx) = mpsc::channel(buffer);
        (Self::from_sender(schema, buffer, tx), rx)
    }

    /// Wrap an existing sender half, paired with a receiver resolved
    /// elsewhere — the [`ChannelRegistry`](crate::registry::ChannelRegistry)
    /// bridge's sink-side constructor.
    pub fn from_sender(schema: Arc<Schema>, buffer: usize, tx: mpsc::Sender<RecordBatch>) -> Self {
        Self {
            tx,
            schema,
            buffer_capacity: buffer,
            queued: VecDeque::with_capacity(buffer.min(64)),
            sent_rows: 0,
        }
    }

    /// Messages accepted by the channel and not yet received.
    ///
    /// `tx.capacity()` is the number of free slots left, so the difference
    /// from the configured capacity is what is still in flight.
    fn queued_messages(&self) -> usize {
        self.buffer_capacity.saturating_sub(self.tx.capacity())
    }
}

#[async_trait]
impl Sink for ChannelSink {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        self.tx
            .send(batch.clone())
            .await
            .map_err(|e| SaciError::generic(format!("ChannelSink: channel send error: {e}")))?;
        self.queued.push_back(self.sent_rows);
        self.sent_rows += batch.num_rows() as u64;
        // Everything ahead of what is still in flight has been received, so
        // drop it: the deque stays bounded by the channel's own capacity.
        let in_flight = self.queued_messages();
        while self.queued.len() > in_flight {
            self.queued.pop_front();
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), SaciError> {
        // Nothing to flush; the receiver reads at its own pace.
        Ok(())
    }

    fn pending_rows(&self) -> Option<usize> {
        let in_flight = self.queued_messages();
        let oldest = self.queued.len().saturating_sub(in_flight);
        // Past the end once the receiver has drained everything written so
        // far, which is a backlog of zero.
        let base = self.queued.get(oldest).copied().unwrap_or(self.sent_rows);
        Some((self.sent_rows - base) as usize)
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

    fn make_batch(schema: Arc<Schema>, n: i32) -> RecordBatch {
        let arr = Arc::new(Int32Array::from_iter_values(0..n));
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    #[tokio::test]
    async fn test_channel_sink_receive_batch() {
        let schema = make_schema();
        let (mut sink, mut rx) = ChannelSink::new(schema.clone(), 4);

        let batch = make_batch(schema.clone(), 5);
        sink.write_batch(&batch).await.unwrap();
        sink.finish().await.unwrap();

        let received = rx.recv().await.unwrap();
        assert_eq!(received.num_rows(), 5);
    }

    #[tokio::test]
    async fn test_channel_sink_multiple_batches() {
        let schema = make_schema();
        let (mut sink, mut rx) = ChannelSink::new(schema.clone(), 8);

        for n in [3i32, 7, 11] {
            sink.write_batch(&make_batch(schema.clone(), n))
                .await
                .unwrap();
        }
        sink.finish().await.unwrap();
        drop(sink);

        let mut total = 0;
        while let Some(b) = rx.recv().await {
            total += b.num_rows();
        }
        assert_eq!(total, 21);
    }

    #[tokio::test]
    async fn test_channel_sink_eof_when_sink_dropped() {
        let schema = make_schema();
        let (sink, mut rx) = ChannelSink::new(schema.clone(), 4);
        drop(sink);
        assert!(rx.recv().await.is_none());
    }

    /// `pending_rows` is a row backlog, not a message count: the `Sink` trait
    /// documents it as rows, `saci-service`'s flow control treats it as rows of
    /// congestion, and `saci_sink_pending_rows` publishes it as rows. A channel
    /// queues whole batches, so the two differ by the batch size.
    #[tokio::test]
    async fn pending_rows_counts_rows_not_queued_batches() {
        let schema = make_schema();
        let (mut sink, mut rx) = ChannelSink::new(schema.clone(), 4);
        assert_eq!(sink.pending_rows(), Some(0));

        sink.write_batch(&make_batch(schema.clone(), 100))
            .await
            .unwrap();
        assert_eq!(sink.pending_rows(), Some(100));

        sink.write_batch(&make_batch(schema.clone(), 25))
            .await
            .unwrap();
        assert_eq!(sink.pending_rows(), Some(125));

        assert_eq!(rx.recv().await.unwrap().num_rows(), 100);
        assert_eq!(
            sink.pending_rows(),
            Some(25),
            "consuming the oldest batch retires exactly its rows"
        );

        assert_eq!(rx.recv().await.unwrap().num_rows(), 25);
        assert_eq!(sink.pending_rows(), Some(0));
    }

    #[tokio::test]
    async fn test_channel_sink_schema_accessor() {
        let schema = make_schema();
        let (sink, _rx) = ChannelSink::new(schema.clone(), 1);
        assert_eq!(sink.schema().field(0).name(), "v");
    }
}
