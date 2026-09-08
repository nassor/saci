//! [`S3Sink`]: rows accumulated in memory, uploaded to S3 as one object when a
//! flush threshold fires.
//!
//! The open object lives in a shared byte buffer behind a
//! [`std::sync::Mutex`], so the `flush.max_age_ms` ticker task and
//! `Sink::write_batch` both reach it. Whichever threshold fires first — enough
//! rows, enough encoded bytes, or enough wall-clock time since the object took
//! its first batch — closes the object, uploads it, and opens the next one.

use std::sync::{Arc, Mutex, PoisonError};

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use saci_core::error::SaciError;
use saci_core::io::sink::Sink;
use saci_transformer::{BatchWriter, Transformer};

use crate::config::{Flush, S3SinkConfig};

/// A `std::io::Write` handle onto a shared byte buffer.
///
/// `Transformer::open_writer` takes ownership of the handle and
/// `BatchWriter::finish` consumes the writer, so the sink keeps a second handle
/// on the same bytes to read the finished object back out — the same trick
/// `FileSink` plays with `File::try_clone`.
#[derive(Clone, Default)]
struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

impl SharedBuffer {
    /// Encoded bytes accumulated so far.
    fn len(&self) -> usize {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).len()
    }

    /// Drain the buffer, leaving it empty for the next object.
    fn take(&self) -> Vec<u8> {
        let mut inner = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        std::mem::take(&mut *inner)
    }
}

impl std::io::Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut inner = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        inner.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Everything both `write_batch` and the ticker read. Immutable after
/// construction, so it needs no lock.
struct SinkShared {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    suffix: String,
    flush: Flush,
    transformer: Arc<dyn Transformer>,
    schema: Arc<Schema>,
}

/// Everything both mutate.
struct SinkState {
    /// `None` before the first batch, between flushes, and after `finish`.
    open: Option<OpenObject>,
    /// Flush counter, into the key's `seq` field.
    seq: u64,
    finished: bool,
}

struct OpenObject {
    writer: Box<dyn BatchWriter>,
    buffer: SharedBuffer,
    rows: usize,
    /// Key timestamp and the `max_age_ms` reference point: when the first batch
    /// landed in this object, not when the sink was built.
    opened_at: DateTime<Utc>,
}

/// S3 [`Sink`]. The transformer encodes; this type owns the object client, the
/// shared byte buffer, and the `flush.max_age_ms` ticker task.
///
/// Rows accumulate in the open object's in-memory buffer until a flush
/// threshold fires or `finish` runs. A sink dropped without `finish` loses the
/// rows in the open object: an unflushed object is not durable, and the source
/// it drained has already advanced.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
///
/// use arrow_schema::Schema;
/// use saci_connector_s3::{Flush, S3ConnectionConfig, S3Sink, S3SinkConfig};
/// use saci_transformer_csv::CsvTransformer;
///
/// let config = S3SinkConfig {
///     connection: S3ConnectionConfig {
///         bucket: "orders".to_string(),
///         endpoint: Some("http://127.0.0.1:9000".to_string()),
///         access_key_id: Some("key".to_string()),
///         secret_access_key: Some("secret".to_string()),
///         allow_http: true,
///         ..Default::default()
///     },
///     prefix: "out".to_string(),
///     suffix: ".csv".to_string(),
///     flush: Flush::default(),
///     schema_fields: Vec::new(),
/// };
/// let sink = S3Sink::new(config, Arc::new(Schema::empty()), Arc::new(CsvTransformer::new(true)))
///     .unwrap();
/// ```
pub struct S3Sink {
    shared: Arc<SinkShared>,
    state: Arc<tokio::sync::Mutex<SinkState>>,
    /// Spawned by the first `write_batch` when `flush.max_age_ms > 0`, aborted
    /// by `finish` and by `Drop`.
    ticker: Option<tokio::task::JoinHandle<()>>,
}

impl S3Sink {
    /// Synchronous and opens no connection, matching `KafkaSource::new`: the
    /// first request happens inside `write_batch`. The ticker task is spawned
    /// from the first `write_batch` too, which is async and therefore
    /// guarantees a runtime handle exists.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when the connection settings are
    /// invalid. No request is made.
    pub fn new(
        config: S3SinkConfig,
        schema: Arc<Schema>,
        transformer: Arc<dyn Transformer>,
    ) -> Result<Self, SaciError> {
        let store = config.connection.build_store("S3Sink")?;
        Ok(Self {
            shared: Arc::new(SinkShared {
                store,
                prefix: config.prefix,
                suffix: config.suffix,
                flush: config.flush,
                transformer,
                schema,
            }),
            state: Arc::new(tokio::sync::Mutex::new(SinkState {
                open: None,
                seq: 0,
                finished: false,
            })),
            ticker: None,
        })
    }

    fn ensure_ticker(&mut self) {
        if self.ticker.is_none() && self.shared.flush.max_age_ms > 0 {
            // Quarter of the budget, floored at 50 ms, so an object is uploaded
            // within 1.25 x max_age_ms of its first batch rather than 2x,
            // without a wakeup storm for a small budget.
            let period =
                std::time::Duration::from_millis((self.shared.flush.max_age_ms / 4).max(50));
            let max_age = chrono::Duration::milliseconds(self.shared.flush.max_age_ms as i64);
            let weak = Arc::downgrade(&self.state);
            let shared = Arc::clone(&self.shared);
            self.ticker = Some(tokio::spawn(async move {
                let mut tick = tokio::time::interval(period);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    // The sink was dropped: nothing left to flush and nothing
                    // to log to.
                    let Some(state) = weak.upgrade() else { return };
                    let mut state = state.lock().await;
                    if state.finished {
                        return;
                    }
                    let due = state
                        .open
                        .as_ref()
                        .is_some_and(|o| Utc::now() - o.opened_at >= max_age);
                    if due && let Err(e) = flush_locked(&shared, &mut state).await {
                        // Nothing to return an error to; a failed timed upload
                        // must not kill the ticker, and `write_batch` will
                        // surface the next one.
                        #[cfg(feature = "tracing")]
                        tracing::warn!(error = %e, "S3Sink: timed flush failed");
                        #[cfg(not(feature = "tracing"))]
                        let _ = e;
                    }
                }
            }));
        }
    }
}

impl Drop for S3Sink {
    fn drop(&mut self) {
        // `finish` already took it; a sink dropped mid-run leaves no task
        // behind, and the rows in the open object go with it (see the type
        // doc).
        if let Some(ticker) = self.ticker.take() {
            ticker.abort();
        }
    }
}

#[async_trait]
impl Sink for S3Sink {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.shared.schema)
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        self.ensure_ticker();
        let mut state = self.state.lock().await;
        if state.finished {
            return Err(SaciError::generic(
                "S3Sink: write_batch called after finish",
            ));
        }
        if state.open.is_none() {
            let buffer = SharedBuffer::default();
            let writer = self
                .shared
                .transformer
                .open_writer(Box::new(buffer.clone()), Arc::clone(&self.shared.schema))?;
            state.open = Some(OpenObject {
                writer,
                buffer,
                rows: 0,
                opened_at: Utc::now(),
            });
        }
        let open = state.open.as_mut().expect("open was just set");
        // Called inline, exactly as `FileSink::write_batch` calls it: the
        // encoder is CPU-bound, not IO-bound, so a blocking thread buys nothing
        // here.
        open.writer.write_batch(batch)?;
        open.rows += batch.num_rows();
        let flush = &self.shared.flush;
        if (flush.max_rows > 0 && open.rows >= flush.max_rows)
            || (flush.max_bytes > 0 && open.buffer.len() >= flush.max_bytes)
        {
            flush_locked(&self.shared, &mut state).await?;
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), SaciError> {
        // Abort the ticker first, so no tick can interleave with the final
        // upload. Idempotent like `FileSink`'s: a second call finds no ticker
        // and no open object.
        if let Some(ticker) = self.ticker.take() {
            ticker.abort();
        }
        let mut state = self.state.lock().await;
        flush_locked(&self.shared, &mut state).await?;
        state.finished = true;
        Ok(())
    }

    fn pending_rows(&self) -> Option<usize> {
        // `pending_rows` cannot await; a contended lock (a flush in flight)
        // reports nothing rather than blocking the scheduler.
        match self.state.try_lock() {
            Ok(state) => state.open.as_ref().map(|o| o.rows),
            Err(_) => None,
        }
    }
}

/// Close, upload and drop the open object. A free function over the two halves
/// rather than a method, so the ticker task can call it while holding the same
/// guard `write_batch` uses.
async fn flush_locked(shared: &SinkShared, state: &mut SinkState) -> Result<(), SaciError> {
    let Some(open) = state.open.take() else {
        return Ok(());
    };
    // A writer that took no rows is dropped without an upload, so neither
    // `finish` nor a ticker tick on an idle sink writes an empty object.
    if open.rows == 0 {
        return Ok(());
    }
    // Consume the writer, flushing the format's own trailer through the shared
    // buffer, then drain the bytes.
    open.writer.finish()?;
    let bytes = open.buffer.take();
    let rows = open.rows;
    let byte_count = bytes.len();
    let key = crate::key::object_key(&shared.prefix, &shared.suffix, open.opened_at, state.seq);
    state.seq += 1;
    shared
        .store
        .put(&key, PutPayload::from(bytes))
        .await
        .map_err(|e| SaciError::generic(format!("S3Sink: put {key}: {e}")))?;
    #[cfg(feature = "tracing")]
    tracing::info!(key = %key, rows, bytes = byte_count, "S3Sink: object uploaded");
    #[cfg(not(feature = "tracing"))]
    let _ = (key, rows, byte_count);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use saci_transformer_csv::CsvTransformer;

    use super::*;

    use crate::S3ConnectionConfig;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn batch(rows: u32) -> RecordBatch {
        let id: ArrayRef = Arc::new(Int64Array::from_iter_values(0..i64::from(rows)));
        let name: ArrayRef = Arc::new(StringArray::from_iter_values(
            (0..rows).map(|i| format!("row-{i}")),
        ));
        RecordBatch::try_new(schema(), vec![id, name]).expect("batch builds")
    }

    fn config() -> S3SinkConfig {
        S3SinkConfig {
            connection: S3ConnectionConfig {
                bucket: "test".to_string(),
                endpoint: Some("http://127.0.0.1:1".to_string()),
                allow_http: true,
                ..Default::default()
            },
            prefix: String::new(),
            suffix: String::new(),
            flush: Flush::default(),
            schema_fields: Vec::new(),
        }
    }

    #[test]
    fn the_shared_buffer_accumulates_and_take_empties_it() {
        let mut buffer = SharedBuffer::default();
        buffer.write_all(b"abc").expect("write");
        buffer.write_all(b"def").expect("write");
        assert_eq!(buffer.len(), 6);
        assert_eq!(buffer.take(), b"abcdef");
        assert_eq!(buffer.len(), 0);
    }

    #[tokio::test]
    async fn write_batch_after_finish_is_the_exact_error() {
        let mut sink =
            S3Sink::new(config(), schema(), Arc::new(CsvTransformer::new(true))).expect("builds");
        sink.finish()
            .await
            .expect("finish on a fresh sink is a no-op");
        let err = sink
            .write_batch(&batch(1))
            .await
            .expect_err("write after finish");
        assert_eq!(err.message(), "S3Sink: write_batch called after finish");
    }

    #[tokio::test]
    async fn finish_on_a_never_written_sink_issues_no_request() {
        // The endpoint is unreachable; any request would fail the test.
        let mut sink =
            S3Sink::new(config(), schema(), Arc::new(CsvTransformer::new(true))).expect("builds");
        sink.finish().await.expect("finish issues no request");
        sink.finish().await.expect("finish is idempotent");
    }
}
