//! [`S3Sink`]: rows accumulated in memory, uploaded to S3 as one object when a
//! flush threshold fires.
//!
//! The open object lives behind a [`tokio::sync::Mutex`], so the
//! `flush.max_age_ms` ticker task and `Sink::write_batch` both reach it.
//! Whichever threshold fires first — enough rows, enough encoded bytes, or
//! enough wall-clock time since the object took its first batch — encodes the
//! object, uploads it, and opens the next one.
//!
//! # Failure semantics
//!
//! The object keeps its accepted batches, and the encoder is only a cache of
//! them. That is what makes every failure leave a state the caller can rely
//! on:
//!
//! - A failed `write_batch` does not keep its own batch, whether the encoder or
//!   the upload failed: the service's retry wrapper re-drives the same batch,
//!   so keeping it would write its rows twice. Rows an earlier call
//!   accepted stay pending until an upload answers `Ok`.
//! - A failed `finish` keeps everything; calling it again uploads it.
//! - `seq` advances only after an upload answers `Ok`, so keys have no gaps. A
//!   retried upload of the same object computes the same key, because the
//!   timestamp is fixed once an upload was sent, so one that landed but
//!   reported failure is overwritten rather than duplicated.
//! - A dropped (cancelled) call is taken back the same way on the next call:
//!   a dropped `write_batch` loses its own batch (its caller never learned the
//!   outcome and re-drives it), a dropped `finish` or ticker upload loses
//!   nothing, and no row is written twice.

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

impl SinkShared {
    /// A writer over a fresh buffer, holding nothing yet.
    fn open_encoder(&self) -> Result<Encoder, SaciError> {
        let buffer = SharedBuffer::default();
        let writer = self
            .transformer
            .open_writer(Box::new(buffer.clone()), Arc::clone(&self.schema))?;
        Ok(Encoder { writer, buffer })
    }

    /// A writer holding exactly `batches`, encoded in order.
    fn encode(&self, batches: &[RecordBatch]) -> Result<Encoder, SaciError> {
        let mut encoder = self.open_encoder()?;
        for batch in batches {
            encoder.writer.write_batch(batch)?;
        }
        Ok(encoder)
    }
}

/// The format's writer and the bytes it has produced so far.
struct Encoder {
    writer: Box<dyn BatchWriter>,
    buffer: SharedBuffer,
}

impl Encoder {
    /// Write the format's trailer and drain the finished object.
    fn finish(self) -> Result<Vec<u8>, SaciError> {
        self.writer.finish()?;
        Ok(self.buffer.take())
    }
}

/// Everything both mutate.
struct SinkState {
    /// `None` before the first batch, after an upload succeeded, and after
    /// `finish`.
    open: Option<Pending>,
    /// Number of objects uploaded, into the next key's `seq` field. It moves
    /// only when an upload answers `Ok`, so keys have no gaps.
    seq: u64,
    /// Rows of the `write_batch` call in flight, which is the last batch of
    /// `open`. Set before that call can be dropped, cleared when it returns, so
    /// a value found on entry belongs to a call that was dropped.
    own_rows: usize,
    finished: bool,
}

impl SinkState {
    /// Add `batch` to the open object and encode it. On an error the batch is
    /// taken back out, so the caller's retry does not write it twice.
    fn accept(&mut self, shared: &SinkShared, batch: &RecordBatch) -> Result<(), SaciError> {
        let open = self.open.get_or_insert_with(Pending::new);
        open.push(batch.clone());
        self.own_rows = batch.num_rows();
        // Called inline, exactly as `FileSink::write_batch` calls it: the
        // encoder is CPU-bound, not IO-bound, so a blocking thread buys nothing
        // here.
        let encoded = match &mut open.encoder {
            Some(encoder) => encoder.writer.write_batch(batch),
            // Nothing encoded yet, or an earlier attempt consumed it: encode
            // the whole object, this batch last.
            None => shared.encode(&open.batches).map(|encoder| {
                open.encoder = Some(encoder);
            }),
        };
        if encoded.is_err() {
            self.take_back();
        }
        encoded
    }

    /// Take back the batch of a `write_batch` call that did not return.
    ///
    /// A dropped call never told its caller whether the batch landed; like a
    /// failed one it is removed, because the caller is expected to re-drive it.
    /// The encoder goes with it: it holds the batch, perhaps half of it, and
    /// the next write rebuilds one from the batches that remain. A dropped
    /// `finish`, and the rows earlier calls accepted, stay.
    fn take_back(&mut self) {
        if self.own_rows == 0 {
            return;
        }
        if let Some(open) = &mut self.open {
            open.batches.pop();
            open.rows -= self.own_rows;
            open.encoder = None;
            // Nothing accepted and nothing ever sent: as if never opened.
            if open.batches.is_empty() && !open.attempted {
                self.open = None;
            }
        }
        self.own_rows = 0;
    }
}

/// The object being accumulated: every accepted batch since the last upload
/// that answered `Ok`. The batches are the source of truth; the encoder is a
/// cache of them, so a batch can be taken back out and a failed upload
/// retried without losing or repeating a row.
struct Pending {
    batches: Vec<RecordBatch>,
    rows: usize,
    /// Mirrors `batches` when it is `Some`. Dropped when a batch comes back
    /// out or an upload consumed it, rebuilt by the next write or upload.
    encoder: Option<Encoder>,
    /// The key's timestamp: when the first batch landed in this object, not
    /// when the sink was built. Fixed once an upload was attempted.
    opened_at: DateTime<Utc>,
    /// The `max_age_ms` reference point: when the object last went from empty
    /// to holding a batch.
    started_at: DateTime<Utc>,
    /// An upload of this object was sent. A request that failed may still have
    /// landed, and the key is `opened_at` plus `seq`, so the retry has to
    /// compute the same one: it then overwrites that object rather than
    /// leaving a second copy of its rows beside it.
    attempted: bool,
}

impl Pending {
    fn new() -> Self {
        let now = Utc::now();
        Self {
            batches: Vec::new(),
            rows: 0,
            encoder: None,
            opened_at: now,
            started_at: now,
            attempted: false,
        }
    }

    fn push(&mut self, batch: RecordBatch) {
        if self.batches.is_empty() {
            let now = Utc::now();
            self.started_at = now;
            if !self.attempted {
                self.opened_at = now;
            }
        }
        self.rows += batch.num_rows();
        self.batches.push(batch);
    }
}

/// S3 [`Sink`]. The transformer encodes; this type owns the object client, the
/// open object and the `flush.max_age_ms` ticker task.
///
/// Batches accumulate in the open object, in memory, until a flush threshold
/// fires or `finish` runs. They are held twice while pending: as the batches
/// themselves (the record of what was accepted, which a failed upload re-encodes
/// from) and as the encoder's bytes (which `flush.max_bytes` measures). A sink
/// dropped without `finish` loses the rows in the open object: an unflushed
/// object is not durable, and the source it drained has already advanced.
///
/// A failed upload loses nothing the caller was told succeeded; see the
/// module's failure semantics.
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
        Ok(Self::from_store(store, config, schema, transformer))
    }

    /// Build over an existing object client; `config.connection` is not
    /// consulted.
    fn from_store(
        store: Arc<dyn ObjectStore>,
        config: S3SinkConfig,
        schema: Arc<Schema>,
        transformer: Arc<dyn Transformer>,
    ) -> Self {
        Self {
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
                own_rows: 0,
                finished: false,
            })),
            ticker: None,
        }
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
                    state.take_back();
                    let due = state
                        .open
                        .as_ref()
                        .is_some_and(|o| o.rows > 0 && Utc::now() - o.started_at >= max_age);
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
        state.take_back();
        if batch.num_rows() == 0 {
            return Ok(());
        }
        state.accept(&self.shared, batch)?;
        let flush = &self.shared.flush;
        let due = state.open.as_ref().is_some_and(|o| {
            (flush.max_rows > 0 && o.rows >= flush.max_rows)
                || (flush.max_bytes > 0
                    && o.encoder
                        .as_ref()
                        .is_some_and(|e| e.buffer.len() >= flush.max_bytes))
        });
        if due {
            // Dropped inside the upload, this call leaves `own_rows` set and
            // the next call takes the batch back out (`take_back`).
            if let Err(e) = flush_locked(&self.shared, &mut state).await {
                // The caller is told this batch failed and re-drives it (the
                // service's retry wrapper does), so it is not kept: holding it
                // too would write its rows twice. Rows an earlier call accepted
                // stay for the next write, the ticker or `finish`.
                state.take_back();
                return Err(e);
            }
        }
        state.own_rows = 0;
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), SaciError> {
        // Abort the ticker first, so no tick can interleave with the final
        // upload. Idempotent like `FileSink`'s: a second call finds no ticker
        // and no open object. A failed or dropped `finish` has lost nothing:
        // everything accepted is still pending, and calling it again uploads it.
        if let Some(ticker) = self.ticker.take() {
            ticker.abort();
        }
        let mut state = self.state.lock().await;
        state.take_back();
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

/// Encode and upload the open object. A free function over the two halves
/// rather than a method, so the ticker task can call it while holding the same
/// guard `write_batch` uses.
///
/// Nothing in `state` changes until the upload answers `Ok`: the batches stay,
/// `seq` stays, and the key stays pinned, so a failed or cancelled call (the
/// future dropped at the `put`) leaves exactly what it found, bar the encoder,
/// which `finish` consumed and the next write rebuilds from the batches.
async fn flush_locked(shared: &SinkShared, state: &mut SinkState) -> Result<(), SaciError> {
    let SinkState {
        open: slot, seq, ..
    } = state;
    let Some(open) = slot.as_mut() else {
        return Ok(());
    };
    // A kept object that holds no rows (its only batch was taken back after a
    // failed upload) is not uploaded, so neither `finish` nor a ticker tick
    // writes an empty object.
    if open.rows == 0 {
        return Ok(());
    }
    // Consume the writer, flushing the format's own trailer through the shared
    // buffer, then drain the bytes. With no live encoder (an earlier attempt
    // consumed it) the same bytes are encoded again from the batches.
    let bytes = match open.encoder.take() {
        Some(encoder) => encoder.finish()?,
        None => shared.encode(&open.batches)?.finish()?,
    };
    let rows = open.rows;
    let byte_count = bytes.len();
    let key = crate::key::object_key(&shared.prefix, &shared.suffix, open.opened_at, *seq);
    // From here the object may exist remotely whatever the call returns, so
    // its key is no longer free to change (see `Pending::attempted`).
    open.attempted = true;
    shared
        .store
        .put(&key, PutPayload::from(bytes))
        .await
        .map_err(|e| SaciError::generic(format!("S3Sink: put {key}: {e}")))?;
    *seq += 1;
    *slot = None;
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
    use std::collections::VecDeque;
    use std::fmt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use futures_util::TryStreamExt as _;
    use futures_util::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutOptions, PutResult,
    };
    use saci_transformer::{BatchWriter, Transformer};

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

    /// What the next `put` does instead of storing the object and answering.
    #[derive(Clone, Copy, Debug)]
    enum Put {
        /// Store nothing, answer with an error.
        Fail,
        /// Store the object, then answer with an error: the upload landed and
        /// the caller was told it did not.
        LostAck,
        /// Never answer.
        Hang,
    }

    /// An in-memory store whose uploads follow a script, one step per `put`;
    /// once the script is empty every upload succeeds.
    #[derive(Debug)]
    struct FlakyStore {
        inner: InMemory,
        script: Mutex<VecDeque<Put>>,
        puts: AtomicUsize,
    }

    impl FlakyStore {
        fn new(script: &[Put]) -> Arc<Self> {
            Arc::new(Self {
                inner: InMemory::new(),
                script: Mutex::new(script.iter().copied().collect()),
                puts: AtomicUsize::new(0),
            })
        }

        fn puts(&self) -> usize {
            self.puts.load(Ordering::SeqCst)
        }

        /// Every stored object, in key order, as `(key, body)`.
        async fn objects(&self) -> Vec<(String, String)> {
            let mut metas: Vec<ObjectMeta> = self.inner.list(None).try_collect().await.unwrap();
            metas.sort_by(|a, b| a.location.cmp(&b.location));
            let mut out = Vec::new();
            for meta in metas {
                let bytes = self.inner.get(&meta.location).await.unwrap().bytes().await;
                out.push((
                    meta.location.to_string(),
                    String::from_utf8(bytes.unwrap().to_vec()).unwrap(),
                ));
            }
            out
        }
    }

    impl fmt::Display for FlakyStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("FlakyStore")
        }
    }

    fn injected() -> object_store::Error {
        object_store::Error::Generic {
            store: "FlakyStore",
            source: "injected failure".into(),
        }
    }

    #[async_trait]
    impl ObjectStore for FlakyStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.puts.fetch_add(1, Ordering::SeqCst);
            let step = self
                .script
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .pop_front();
            match step {
                None => self.inner.put_opts(location, payload, opts).await,
                Some(Put::Fail) => Err(injected()),
                Some(Put::LostAck) => {
                    self.inner.put_opts(location, payload, opts).await?;
                    Err(injected())
                }
                Some(Put::Hang) => std::future::pending().await,
            }
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Writes each row's `id` on its own line. A batch holding id 99 writes
    /// half a line and then fails, which is what a mid-batch encode error
    /// leaves behind.
    struct LineTransformer;

    struct LineWriter(Box<dyn Write + Send>);

    impl BatchWriter for LineWriter {
        fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column");
            for id in ids.iter().flatten() {
                if id == 99 {
                    self.0.write_all(b"99-par").expect("in-memory write");
                    return Err(SaciError::generic("encode failed mid-batch"));
                }
                writeln!(self.0, "{id}").expect("in-memory write");
            }
            Ok(())
        }

        fn finish(self: Box<Self>) -> Result<(), SaciError> {
            Ok(())
        }
    }

    impl Transformer for LineTransformer {
        fn format(&self) -> &'static str {
            "lines"
        }

        fn open_writer(
            &self,
            output: Box<dyn Write + Send>,
            _schema: Arc<Schema>,
        ) -> Result<Box<dyn BatchWriter>, SaciError> {
            Ok(Box::new(LineWriter(output)))
        }
    }

    fn ids(ids: &[i64]) -> RecordBatch {
        let id: ArrayRef = Arc::new(Int64Array::from(ids.to_vec()));
        let name: ArrayRef = Arc::new(StringArray::from_iter_values(ids.iter().map(|_| "r")));
        RecordBatch::try_new(schema(), vec![id, name]).expect("batch builds")
    }

    fn flush(max_rows: usize) -> Flush {
        Flush {
            max_rows,
            max_bytes: 0,
            max_age_ms: 0,
        }
    }

    fn sink_over(
        store: &Arc<FlakyStore>,
        flush: Flush,
        transformer: Arc<dyn Transformer>,
    ) -> S3Sink {
        let mut config = config();
        config.flush = flush;
        S3Sink::from_store(store.clone(), config, schema(), transformer)
    }

    fn lines() -> Arc<dyn Transformer> {
        Arc::new(LineTransformer)
    }

    #[tokio::test]
    async fn a_failed_upload_does_not_keep_the_batch_that_triggered_it() {
        let store = FlakyStore::new(&[Put::Fail]);
        let mut sink = sink_over(&store, flush(2), lines());
        sink.write_batch(&ids(&[1])).await.expect("buffered");

        sink.write_batch(&ids(&[2]))
            .await
            .expect_err("the upload fails");
        assert_eq!(sink.pending_rows(), Some(1), "only the accepted row stays");

        sink.finish().await.expect("the retry uploads");
        let objects = store.objects().await;
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].1, "1\n", "the failed call's row is not written");
    }

    #[tokio::test]
    async fn rows_accepted_before_a_failed_upload_are_uploaded_by_the_retry() {
        let store = FlakyStore::new(&[Put::Fail]);
        let mut sink = sink_over(&store, flush(2), lines());
        sink.write_batch(&ids(&[1])).await.expect("buffered");
        sink.write_batch(&ids(&[2]))
            .await
            .expect_err("the upload fails");

        // The service's retry wrapper re-drives the same batch.
        sink.write_batch(&ids(&[2])).await.expect("the retry lands");
        sink.finish().await.expect("nothing left");
        let objects = store.objects().await;
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].1, "1\n2\n");
    }

    #[tokio::test]
    async fn a_failed_finish_keeps_every_row() {
        let store = FlakyStore::new(&[Put::Fail]);
        let mut sink = sink_over(&store, flush(0), lines());
        sink.write_batch(&ids(&[1])).await.expect("buffered");
        sink.write_batch(&ids(&[2])).await.expect("buffered");

        sink.finish().await.expect_err("the upload fails");
        assert_eq!(sink.pending_rows(), Some(2));
        sink.finish().await.expect("the retry uploads");
        let objects = store.objects().await;
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].1, "1\n2\n");
    }

    #[tokio::test]
    async fn a_failed_upload_leaves_no_gap_in_the_keys() {
        let store = FlakyStore::new(&[Put::Fail]);
        let mut sink = sink_over(&store, flush(1), lines());
        sink.write_batch(&ids(&[1])).await.expect_err("fails");
        sink.write_batch(&ids(&[1])).await.expect("retry lands");
        sink.write_batch(&ids(&[2])).await.expect("next object");
        sink.finish().await.expect("finish");

        let objects = store.objects().await;
        let keys: Vec<&str> = objects.iter().map(|(key, _)| key.as_str()).collect();
        assert_eq!(keys.len(), 2, "{keys:?}");
        assert!(keys[0].ends_with("-000000"), "{keys:?}");
        assert!(keys[1].ends_with("-000001"), "{keys:?}");
        assert_eq!(objects[0].1, "1\n");
        assert_eq!(objects[1].1, "2\n");
    }

    #[tokio::test]
    async fn an_upload_that_landed_but_reported_failure_is_overwritten_by_the_retry() {
        let store = FlakyStore::new(&[Put::LostAck]);
        let mut sink = sink_over(&store, flush(1), lines());
        sink.write_batch(&ids(&[1]))
            .await
            .expect_err("the answer is lost");
        sink.write_batch(&ids(&[1])).await.expect("retry lands");
        sink.finish().await.expect("finish");

        let objects = store.objects().await;
        assert_eq!(objects.len(), 1, "same key, so one object: {objects:?}");
        assert_eq!(objects[0].1, "1\n");
    }

    #[tokio::test]
    async fn a_write_dropped_inside_its_upload_is_taken_back_by_the_next_call() {
        let store = FlakyStore::new(&[Put::Hang]);
        let mut sink = sink_over(&store, flush(2), lines());
        sink.write_batch(&ids(&[1])).await.expect("buffered");
        let dropped =
            tokio::time::timeout(Duration::from_millis(50), sink.write_batch(&ids(&[2]))).await;
        assert!(dropped.is_err(), "the upload never answers");

        // The caller re-drives its batch: it is written once, with the row an
        // earlier call had accepted.
        sink.write_batch(&ids(&[2])).await.expect("re-driven");
        sink.finish().await.expect("finish");
        let objects = store.objects().await;
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].1, "1\n2\n");
    }

    #[tokio::test]
    async fn a_finish_dropped_inside_its_upload_keeps_every_row() {
        let store = FlakyStore::new(&[Put::Hang]);
        let mut sink = sink_over(&store, flush(0), lines());
        sink.write_batch(&ids(&[1])).await.expect("buffered");
        sink.write_batch(&ids(&[2])).await.expect("buffered");
        let dropped = tokio::time::timeout(Duration::from_millis(50), sink.finish()).await;
        assert!(dropped.is_err(), "the upload never answers");

        sink.finish().await.expect("finish again");
        let objects = store.objects().await;
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].1, "1\n2\n");
    }

    #[tokio::test]
    async fn a_failed_timed_upload_keeps_the_rows_for_the_next_tick() {
        let store = FlakyStore::new(&[Put::Fail]);
        let flush = Flush {
            max_rows: 0,
            max_bytes: 0,
            max_age_ms: 100,
        };
        let mut sink = sink_over(&store, flush, lines());
        sink.write_batch(&ids(&[1])).await.expect("buffered");

        let mut objects = Vec::new();
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            objects = store.objects().await;
            if !objects.is_empty() {
                break;
            }
        }
        assert!(store.puts() >= 2, "the ticker retried: {}", store.puts());
        assert_eq!(objects.len(), 1, "a later tick uploads the row");
        assert_eq!(objects[0].1, "1\n");
        sink.finish().await.expect("finish");
    }

    #[tokio::test]
    async fn an_encode_failure_leaves_no_half_written_batch_behind() {
        let store = FlakyStore::new(&[]);
        let mut sink = sink_over(&store, flush(0), lines());
        sink.write_batch(&ids(&[1])).await.expect("buffered");
        sink.write_batch(&ids(&[2, 99]))
            .await
            .expect_err("the encoder fails partway");
        assert_eq!(sink.pending_rows(), Some(1));

        sink.write_batch(&ids(&[2, 3])).await.expect("re-driven");
        sink.finish().await.expect("finish");
        let objects = store.objects().await;
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].1, "1\n2\n3\n");
    }
}
