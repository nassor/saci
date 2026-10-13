//! [`FileSink`]: a local file written through a transformer.
//!
//! # A batch lands once or not at all
//!
//! [`Sink::write_batch`] is retried by the service's `RetryingSink` and may be
//! dropped mid-call, so a call that returns an error, or never returns, must
//! leave the file as it found it. Before each batch the sink records the
//! file's length and how many bytes its writer has put in the file so far.
//! After the writer has encoded the batch and flushed it to the handle, the
//! sink syncs; if the write, the flush or the sync fails the file is cut back
//! to the recorded length, which removes both a whole batch that was written
//! but not synced and a half-written record.
//!
//! The cut is not enough on its own, because the writer's buffers and state
//! may hold part of that batch, or (a CSV header, an Avro or Arrow IPC
//! preamble, a Parquet row group) the writer may believe bytes are in the file
//! that the cut removed. So the writer is discarded and:
//!
//! - when no batch had landed yet, the file is cut back to where the batch
//!   started and a new writer is opened, which is the state the first call
//!   started from, whatever the format. That holds because no format here
//!   writes its header or magic number at open; a writer that did would
//!   repeat it, so the sink refuses instead;
//! - when earlier batches are in the file and the format is
//!   [`restartable`](saci_transformer::BatchWriter::restartable) (ndjson, csv
//!   without a header), a new writer carries on after them;
//! - otherwise (csv with a header, avro, arrow-ipc, parquet) the sink cuts the
//!   file back to the last whole batch and refuses every later call with an
//!   error saying so, because a retry could only duplicate or corrupt.
//!
//! The recovery runs at the top of the next call when a dropped future or a
//! failed cut-back left it undone, so the same rule covers cancellation.
//!
//! # Other writers
//!
//! The writer's handle is opened in append mode, so another process may append
//! to the same file and its bytes land intact between batches. The cut goes
//! through a second handle opened for writing without `append`, because
//! Windows withholds the right to shorten a file from an append-only handle.
//! A cut would also remove bytes another process appended after ours, so the
//! sink counts what its writer put in the file and cuts only when the file is
//! exactly as long as that count says. When it is not, the sink leaves the
//! file alone and refuses every later call.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;

use saci_core::error::SaciError;
use saci_core::io::sink::Sink;
use saci_transformer::{BatchWriter, Transformer};

/// Local-file [`Sink`]. The transformer encodes; this type owns the file
/// handle.
///
/// The writer is held in an `Option` because
/// [`BatchWriter::finish`] consumes it: a
/// format with a footer cannot be written to after its footer is out.
///
/// Every batch is flushed and synced to the file's directory entry before the
/// next one lands. Windows only flushes a held-open file's size metadata
/// lazily, so without the sync a long-running stream shows a 0-byte file until
/// the sink closes, and the last written batch is not yet durable. A batch
/// that fails on the way is taken back out of the file; see the module
/// documentation.
pub struct FileSink {
    writer: Option<Box<dyn BatchWriter>>,
    /// The append-mode handle every writer writes through, as clones. `None`
    /// once the sink has finished.
    append: Option<std::fs::File>,
    /// A second handle on the same file, opened for writing without `append`.
    /// It measures and shortens the file and syncs each batch after the
    /// writer's flush; the writer never sees it.
    file: Option<std::fs::File>,
    /// Bytes the writers have put in the file, whether or not a cut later
    /// removed them. Counted by [`Counting`].
    written: Arc<AtomicU64>,
    /// `written` when the current writer opened: what it wrote before its
    /// first batch is its preamble.
    writer_base: u64,
    transformer: Arc<dyn Transformer>,
    schema: Arc<Schema>,
    path: PathBuf,
    /// Whether any batch has fully landed since the sink opened.
    committed: bool,
    /// A batch that started and was neither completed nor taken back out.
    pending: Option<Pending>,
    /// Why the sink accepts no more batches, once recovery found no way to
    /// carry on.
    refused: Option<String>,
    /// Test hook: how many of the next `sync_data` calls fail.
    #[cfg(test)]
    fail_syncs: u32,
    /// Test hook: how many of the next cut-backs fail.
    #[cfg(test)]
    fail_cuts: u32,
}

/// What a batch started from, and what to do about it. The plan is fixed the
/// first time recovery looks at the writer, so a cut that fails and is retried
/// on the next call still follows it.
#[derive(Clone, Copy)]
struct Pending {
    /// The file's length when the batch started.
    len: u64,
    /// `written` when the batch started.
    written: u64,
    plan: Option<Plan>,
}

#[derive(Clone, Copy)]
struct Plan {
    /// Length to cut the file back to.
    target: u64,
    /// Whether a new writer opens afterwards; if not, the sink refuses.
    reopen: bool,
}

/// The handle given to a transformer: counts the bytes that reach the file.
struct Counting {
    file: std::fs::File,
    written: Arc<AtomicU64>,
}

impl Write for Counting {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.file.write(buf)?;
        self.written.fetch_add(n as u64, Ordering::SeqCst);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

impl FileSink {
    /// Open `path` for appending and hand the handle to `transformer`.
    ///
    /// The file is created when it is missing, and no existing byte is
    /// removed: every batch lands after what is already there. A container
    /// format with a footer, `parquet` and `avro`, cannot be appended to a
    /// non-empty file and stay readable, so those want
    /// [`FileSink::create_truncating`] or a path of their own.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the file cannot be opened, or the
    /// transformer's own error when it cannot write this handle.
    pub fn create(
        path: &Path,
        transformer: Arc<dyn Transformer>,
        schema: Arc<Schema>,
    ) -> Result<Self, SaciError> {
        Self::open_with(
            path,
            std::fs::OpenOptions::new().create(true).write(true),
            |path, e| SaciError::generic(format!("FileSink: cannot open {path:?} for append: {e}")),
            transformer,
            schema,
        )
    }

    /// Replace `path`: create it when missing, empty it when it exists, then
    /// hand the handle to `transformer`.
    ///
    /// This is the opt-in mode behind the sink factory's `truncate` key. It is
    /// what a run whose output must hold this run's rows alone wants, and what
    /// a footer format writing to a path that may already exist needs.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the file cannot be created, or the
    /// transformer's own error when it cannot write this handle.
    pub fn create_truncating(
        path: &Path,
        transformer: Arc<dyn Transformer>,
        schema: Arc<Schema>,
    ) -> Result<Self, SaciError> {
        Self::open_with(
            path,
            std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true),
            |path, e| SaciError::generic(format!("FileSink: cannot create {path:?}: {e}")),
            transformer,
            schema,
        )
    }

    /// Open `path` twice and wire the handles up: `options` (which create, and
    /// maybe truncate, and never append) for the sink's own handle, then an
    /// append-mode handle for the transformer's writer.
    ///
    /// `on_open_error` names the open mode, so a failed open says whether it
    /// was the appending or the replacing one.
    fn open_with(
        path: &Path,
        options: &std::fs::OpenOptions,
        on_open_error: fn(&Path, &std::io::Error) -> SaciError,
        transformer: Arc<dyn Transformer>,
        schema: Arc<Schema>,
    ) -> Result<Self, SaciError> {
        let file = options.open(path).map_err(|e| on_open_error(path, &e))?;
        let append = std::fs::OpenOptions::new()
            .append(true)
            .open(path)
            .map_err(|e| on_open_error(path, &e))?;
        let mut sink = Self {
            writer: None,
            append: Some(append),
            file: Some(file),
            written: Arc::new(AtomicU64::new(0)),
            writer_base: 0,
            transformer,
            schema,
            path: path.to_path_buf(),
            committed: false,
            pending: None,
            refused: None,
            #[cfg(test)]
            fail_syncs: 0,
            #[cfg(test)]
            fail_cuts: 0,
        };
        sink.writer = Some(sink.open_writer()?);
        Ok(sink)
    }

    /// A new writer over a clone of the append handle.
    fn open_writer(&mut self) -> Result<Box<dyn BatchWriter>, SaciError> {
        let file = self
            .append
            .as_ref()
            .ok_or_else(|| SaciError::generic("FileSink: write_batch called after finish"))?
            .try_clone()
            .map_err(|e| {
                SaciError::generic(format!(
                    "FileSink: cannot clone handle for {:?}: {e}",
                    self.path
                ))
            })?;
        self.writer_base = self.written.load(Ordering::SeqCst);
        self.transformer.open_writer(
            Box::new(Counting {
                file,
                written: Arc::clone(&self.written),
            }),
            Arc::clone(&self.schema),
        )
    }

    fn handle(&self) -> Result<&std::fs::File, SaciError> {
        self.file
            .as_ref()
            .ok_or_else(|| SaciError::generic("FileSink: write_batch called after finish"))
    }

    fn len(&self) -> Result<u64, SaciError> {
        self.handle()?
            .metadata()
            .map(|meta| meta.len())
            .map_err(|e| {
                SaciError::generic(format!("FileSink: cannot measure {:?}: {e}", self.path))
            })
    }

    fn sync_data(&mut self) -> Result<(), SaciError> {
        #[cfg(test)]
        if self.fail_syncs > 0 {
            self.fail_syncs -= 1;
            return Err(SaciError::generic("FileSink: sync failed: injected"));
        }
        self.handle()?
            .sync_data()
            .map_err(|e| SaciError::generic(format!("FileSink: sync failed: {e}")))
    }

    /// Shorten the file to `len` through the non-append handle.
    fn cut_to(&mut self, len: u64) -> std::io::Result<()> {
        #[cfg(test)]
        if self.fail_cuts > 0 {
            self.fail_cuts -= 1;
            return Err(std::io::Error::other("injected cut failure"));
        }
        self.file
            .as_ref()
            .ok_or_else(|| std::io::Error::other("the sink has finished"))?
            .set_len(len)
    }

    /// Finish what a failed or dropped batch left undone, if anything.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the file cannot be cut back or the
    /// writer cannot be reopened; the work stays pending and the next call
    /// tries again.
    fn recover(&mut self) -> Result<(), SaciError> {
        let Some(mut pending) = self.pending else {
            return Ok(());
        };
        // The plan reads the writer, so it is made first and then kept.
        let plan = match pending.plan {
            Some(plan) => plan,
            None => {
                let plan = self.plan(&pending);
                pending.plan = Some(plan);
                self.pending = Some(pending);
                plan
            }
        };
        // The old writer goes next: dropping it flushes whatever its buffer
        // holds, and that must reach the file before the cut, not after.
        drop(self.writer.take());
        // The file is as long as our own writes say, or somebody else has
        // appended to it, and a cut would take their bytes along.
        let expected = pending.len + (self.written.load(Ordering::SeqCst) - pending.written);
        if self.len()? != expected {
            self.refused = Some(format!(
                "another writer appended to {:?} while a batch was in flight, so the failed \
                 batch was left in place rather than cut away with their bytes; it cannot be \
                 retried and this sink takes no more batches",
                self.path
            ));
            self.pending = None;
            return Ok(());
        }
        self.cut_to(plan.target).map_err(|e| {
            SaciError::generic(format!(
                "FileSink: cannot take the failed batch back out of {:?}: {e}",
                self.path
            ))
        })?;
        if plan.reopen {
            self.writer = Some(self.open_writer()?);
        } else {
            self.refused = Some(format!(
                "the failed batch was taken back out of {:?}, but the {} writer cannot resume \
                 in the middle of a file, so it cannot be retried and this sink takes no more \
                 batches",
                self.path,
                self.transformer.format()
            ));
        }
        self.pending = None;
        Ok(())
    }

    /// What recovery does for a batch that started as `pending` says.
    fn plan(&self, pending: &Pending) -> Plan {
        if self.committed {
            Plan {
                target: pending.len,
                reopen: self.writer.as_ref().is_some_and(|w| w.restartable()),
            }
        } else {
            // Nothing has landed, so a new writer is the state the first batch
            // started from, provided the old one wrote nothing at open: every
            // format in this workspace holds its header or magic number back
            // for the first batch. A writer that did write one would repeat it.
            Plan {
                target: pending.len,
                reopen: pending.written == self.writer_base,
            }
        }
    }

    /// Refuse a call on a finished or given-up sink, and settle any batch an
    /// earlier call left half done.
    fn begin(&mut self) -> Result<(), SaciError> {
        self.handle()?;
        self.recover()?;
        match &self.refused {
            Some(why) => Err(SaciError::generic(format!("FileSink: {why}"))),
            None => Ok(()),
        }
    }

    /// Record where the batch starts, then encode, flush and sync it. The
    /// record is made before anything is written, so a call that is dropped
    /// part way through is still undone by the next one.
    fn encode(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        self.pending = Some(Pending {
            len: self.len()?,
            written: self.written.load(Ordering::SeqCst),
            plan: None,
        });
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| SaciError::generic("FileSink: no writer"))?;
        writer.write_batch(batch)?;
        // The writer's own buffers go down to the file, so its length is where
        // the writer stands; sync_data then forces the file's size metadata
        // out of the OS cache, so Explorer and `dir` show the batch that just
        // landed instead of the creation-time size.
        writer.flush()?;
        self.sync_data()
    }
}

#[async_trait]
impl Sink for FileSink {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        self.begin()?;
        match self.encode(batch) {
            Ok(()) => {
                self.pending = None;
                self.committed = true;
                Ok(())
            }
            Err(error) => {
                // A failed cut stays pending for the next call; say so here
                // too, so the caller does not read the batch as taken back.
                self.recover().map_err(|cut| {
                    SaciError::generic(format!("{error}; and the rollback failed: {cut}"))
                })?;
                Err(error)
            }
        }
    }

    async fn finish(&mut self) -> Result<(), SaciError> {
        // Idempotent: a second call has no writer left and nothing to flush.
        if self.file.is_none() {
            return Ok(());
        }
        self.begin()?;
        let Some(writer) = self.writer.take() else {
            return Ok(());
        };
        let finished = writer.finish().and_then(|()| self.sync_data());
        if let Err(error) = &finished {
            self.refused = Some(format!("finish failed ({error}); the output is incomplete"));
        }
        if finished.is_ok() {
            // The footer (parquet) just reached the OS and is durable; release
            // the handles. After a failure they stay, so a second call
            // reports the refusal instead of passing for a finished sink.
            self.file = None;
            self.append = None;
        }
        finished
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use saci_transformer::BatchWriter;
    use saci_transformer_csv::CsvTransformer;
    use saci_transformer_ndjson::NdjsonTransformer;
    use saci_transformer_parquet::ParquetTransformer;
    use tempfile::TempDir;

    use super::*;
    use crate::FileSource;
    use saci_core::io::source::Source;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn batch(ids: &[i64]) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(ids.to_vec()))])
            .expect("batch")
    }

    /// The handle shared between the real writer and the fault injector, so
    /// the injector can leave a half-written fragment in the file.
    #[derive(Clone)]
    struct SharedOutput(Arc<Mutex<Box<dyn Write + Send>>>);

    impl Write for SharedOutput {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("output lock").write(buf)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.0.lock().expect("output lock").flush()
        }
    }

    /// Wraps a real transformer. The writer's `n`th `write_batch` call (counted
    /// across every writer this transformer opened) writes its batch for real,
    /// then appends a half record and fails, the way a disk error part way
    /// through a batch looks from outside.
    struct Faulty {
        inner: Arc<dyn Transformer>,
        fail_calls: Vec<usize>,
        calls: Arc<AtomicUsize>,
        /// A file another process appends `INTRUDER` to during each failing
        /// call, after the sink's own bytes have landed.
        intruder: Option<PathBuf>,
    }

    const INTRUDER: &[u8] = b"{\"id\":100}\n";

    impl Faulty {
        fn new(inner: Arc<dyn Transformer>, fail_calls: &[usize]) -> Arc<Self> {
            Arc::new(Self {
                inner,
                fail_calls: fail_calls.to_vec(),
                calls: Arc::new(AtomicUsize::new(0)),
                intruder: None,
            })
        }

        fn with_intruder(
            inner: Arc<dyn Transformer>,
            fail_calls: &[usize],
            path: &Path,
        ) -> Arc<Self> {
            Arc::new(Self {
                inner,
                fail_calls: fail_calls.to_vec(),
                calls: Arc::new(AtomicUsize::new(0)),
                intruder: Some(path.to_path_buf()),
            })
        }
    }

    impl Transformer for Faulty {
        fn format(&self) -> &'static str {
            self.inner.format()
        }

        fn open_writer(
            &self,
            output: Box<dyn Write + Send>,
            schema: Arc<Schema>,
        ) -> Result<Box<dyn BatchWriter>, SaciError> {
            let shared = SharedOutput(Arc::new(Mutex::new(output)));
            let inner = self.inner.open_writer(Box::new(shared.clone()), schema)?;
            Ok(Box::new(FaultyWriter {
                inner,
                output: shared,
                fail_calls: self.fail_calls.clone(),
                calls: Arc::clone(&self.calls),
                intruder: self.intruder.clone(),
            }))
        }
    }

    struct FaultyWriter {
        inner: Box<dyn BatchWriter>,
        output: SharedOutput,
        fail_calls: Vec<usize>,
        calls: Arc<AtomicUsize>,
        intruder: Option<PathBuf>,
    }

    impl BatchWriter for FaultyWriter {
        fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.write_batch(batch)?;
            if self.fail_calls.contains(&call) {
                self.inner.flush()?;
                self.output.write_all(b"{\"id\":9").expect("fragment");
                if let Some(path) = &self.intruder {
                    append_externally(path, INTRUDER);
                }
                return Err(SaciError::generic("injected write failure"));
            }
            Ok(())
        }

        fn flush(&mut self) -> Result<(), SaciError> {
            self.inner.flush()
        }

        fn restartable(&self) -> bool {
            self.inner.restartable()
        }

        fn finish(self: Box<Self>) -> Result<(), SaciError> {
            self.inner.finish()
        }
    }

    fn ndjson() -> Arc<dyn Transformer> {
        Arc::new(NdjsonTransformer::new(100))
    }

    fn csv(has_headers: bool) -> Arc<dyn Transformer> {
        Arc::new(CsvTransformer::new(has_headers))
    }

    /// Append to `path` the way another process would: its own handle.
    fn append_externally(path: &Path, bytes: &[u8]) {
        std::fs::OpenOptions::new()
            .append(true)
            .open(path)
            .expect("external open")
            .write_all(bytes)
            .expect("external append");
    }

    fn text(path: &Path) -> String {
        std::fs::read_to_string(path).expect("read output")
    }

    #[tokio::test]
    async fn a_failed_write_leaves_no_fragment_and_the_retry_lands_once() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("out.ndjson");
        let mut sink =
            FileSink::create(&path, Faulty::new(ndjson(), &[1]), schema()).expect("sink");

        sink.write_batch(&batch(&[1])).await.expect("first");
        sink.write_batch(&batch(&[2]))
            .await
            .expect_err("second fails after its bytes landed");
        sink.write_batch(&batch(&[2])).await.expect("retry");
        sink.write_batch(&batch(&[3])).await.expect("third");
        sink.finish().await.expect("finish");

        assert_eq!(
            text(&path),
            "{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n",
            "the batch is in the file once and no half record remains"
        );
    }

    #[tokio::test]
    async fn a_failed_sync_takes_the_batch_back_out_before_the_retry() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("out.ndjson");
        let mut sink = FileSink::create(&path, ndjson(), schema()).expect("sink");

        sink.write_batch(&batch(&[1])).await.expect("first");
        sink.fail_syncs = 1;
        sink.write_batch(&batch(&[2]))
            .await
            .expect_err("sync fails after the bytes were written");
        assert_eq!(
            text(&path),
            "{\"id\":1}\n",
            "the failed call hands back a file without its batch"
        );
        sink.write_batch(&batch(&[2])).await.expect("retry");
        sink.finish().await.expect("finish");

        assert_eq!(text(&path), "{\"id\":1}\n{\"id\":2}\n");
    }

    #[tokio::test]
    async fn a_failed_first_csv_batch_does_not_lose_or_repeat_the_header() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("out.csv");
        let mut sink =
            FileSink::create(&path, Faulty::new(csv(true), &[0]), schema()).expect("sink");

        sink.write_batch(&batch(&[1, 2]))
            .await
            .expect_err("first batch fails after the header landed");
        sink.write_batch(&batch(&[1, 2])).await.expect("retry");
        sink.write_batch(&batch(&[3])).await.expect("second");
        sink.finish().await.expect("finish");

        assert_eq!(text(&path), "id\n1\n2\n3\n");
    }

    #[tokio::test]
    async fn a_later_batch_of_a_headed_csv_cannot_be_retried_and_says_so() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("out.csv");
        let mut sink =
            FileSink::create(&path, Faulty::new(csv(true), &[1]), schema()).expect("sink");

        sink.write_batch(&batch(&[1])).await.expect("first");
        sink.write_batch(&batch(&[2])).await.expect_err("second");
        assert_eq!(text(&path), "id\n1\n", "only whole earlier batches remain");

        // A fresh csv writer would write the header line again; the sink
        // refuses rather than append a duplicate or a headerless run.
        let err = sink
            .write_batch(&batch(&[2]))
            .await
            .expect_err("the retry is refused");
        assert!(
            err.message().contains("cannot be retried"),
            "the refusal names the cause, got: {err}"
        );
        sink.finish().await.expect_err("finish refuses too");
        assert_eq!(text(&path), "id\n1\n");
    }

    #[tokio::test]
    async fn a_failed_first_parquet_batch_is_retried_into_a_readable_file() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("out.parquet");
        let transformer = Faulty::new(Arc::new(ParquetTransformer::new()), &[0]);
        let mut sink = FileSink::create_truncating(&path, transformer, schema()).expect("sink");

        sink.write_batch(&batch(&[1, 2]))
            .await
            .expect_err("first batch fails");
        sink.write_batch(&batch(&[1, 2])).await.expect("retry");
        sink.write_batch(&batch(&[3])).await.expect("second");
        sink.finish().await.expect("finish");

        let mut source = FileSource::open_async(&path, Arc::new(ParquetTransformer::new()), None)
            .await
            .expect("parquet reads back");
        let mut ids = Vec::new();
        while let Some(rows) = source.next_batch().await.expect("read") {
            let column = rows
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64")
                .clone();
            ids.extend(column.iter().flatten());
        }
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn a_failed_cut_back_is_retried_by_the_next_call() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("out.ndjson");
        let mut sink =
            FileSink::create(&path, Faulty::new(ndjson(), &[1]), schema()).expect("sink");

        sink.write_batch(&batch(&[1])).await.expect("first");
        sink.fail_cuts = 1;
        let err = sink
            .write_batch(&batch(&[2]))
            .await
            .expect_err("the write fails and so does the cut");
        assert!(
            err.message().contains("rollback failed"),
            "the error says the batch is still in the file, got: {err}"
        );

        sink.write_batch(&batch(&[2]))
            .await
            .expect("the next call cuts first, then writes");
        sink.finish().await.expect("finish");

        assert_eq!(text(&path), "{\"id\":1}\n{\"id\":2}\n");
    }

    #[tokio::test]
    async fn a_dropped_call_is_undone_by_the_next_one() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("out.ndjson");
        let mut sink = FileSink::create(&path, ndjson(), schema()).expect("sink");

        sink.write_batch(&batch(&[1])).await.expect("first");
        // A write_batch future dropped after its bytes landed: the call began
        // and encoded, and never settled.
        sink.begin().expect("begin");
        sink.encode(&batch(&[2])).expect("encode");
        assert_eq!(text(&path), "{\"id\":1}\n{\"id\":2}\n");

        sink.write_batch(&batch(&[2])).await.expect("redriven");
        sink.finish().await.expect("finish");

        assert_eq!(text(&path), "{\"id\":1}\n{\"id\":2}\n");
    }

    #[tokio::test]
    async fn a_dropped_call_on_an_appending_sink_keeps_the_existing_bytes() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("out.ndjson");
        std::fs::write(&path, "{\"id\":0}\n").expect("seed file");
        let mut sink = FileSink::create(&path, ndjson(), schema()).expect("sink");

        sink.begin().expect("begin");
        sink.encode(&batch(&[1])).expect("encode");
        sink.write_batch(&batch(&[1])).await.expect("redriven");
        sink.finish().await.expect("finish");

        assert_eq!(text(&path), "{\"id\":0}\n{\"id\":1}\n");
    }

    #[tokio::test]
    async fn another_process_appending_between_batches_lands_intact() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("out.ndjson");
        let mut sink = FileSink::create(&path, ndjson(), schema()).expect("sink");

        sink.write_batch(&batch(&[1])).await.expect("first");
        append_externally(&path, INTRUDER);
        sink.write_batch(&batch(&[2])).await.expect("second");
        sink.finish().await.expect("finish");

        assert_eq!(
            text(&path),
            "{\"id\":1}\n{\"id\":100}\n{\"id\":2}\n",
            "the writer appends after the other process's bytes instead of over them"
        );
    }

    #[tokio::test]
    async fn a_cut_back_keeps_bytes_another_process_appended_before_the_batch() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("out.ndjson");
        let mut sink =
            FileSink::create(&path, Faulty::new(ndjson(), &[1]), schema()).expect("sink");

        sink.write_batch(&batch(&[1])).await.expect("first");
        append_externally(&path, INTRUDER);
        sink.write_batch(&batch(&[2])).await.expect_err("second");
        sink.write_batch(&batch(&[2])).await.expect("retry");
        sink.finish().await.expect("finish");

        assert_eq!(
            text(&path),
            "{\"id\":1}\n{\"id\":100}\n{\"id\":2}\n",
            "the batch start was measured after the other process wrote"
        );
    }

    #[tokio::test]
    async fn bytes_appended_during_a_failed_batch_are_never_cut_away() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("out.ndjson");
        std::fs::write(&path, "").expect("create");
        let transformer = Faulty::with_intruder(ndjson(), &[1], &path);
        let mut sink = FileSink::create(&path, transformer, schema()).expect("sink");

        sink.write_batch(&batch(&[1])).await.expect("first");
        sink.write_batch(&batch(&[2]))
            .await
            .expect_err("second fails while another process appends");
        let after_failure = text(&path);
        assert!(
            after_failure.ends_with("{\"id\":100}\n"),
            "their bytes are still there, got: {after_failure:?}"
        );

        let err = sink
            .write_batch(&batch(&[2]))
            .await
            .expect_err("a retry would duplicate, so the sink refuses");
        assert!(
            err.message().contains("another writer appended"),
            "the refusal names the cause, got: {err}"
        );
        sink.finish().await.expect_err("finish refuses too");
        assert_eq!(text(&path), after_failure, "nothing was cut or added");
    }

    #[tokio::test]
    async fn no_format_writes_to_the_file_before_its_first_batch() {
        let dir = TempDir::new().expect("temp dir");
        let formats: [(&str, Arc<dyn Transformer>); 3] = [
            ("ndjson", ndjson()),
            ("csv", csv(true)),
            ("parquet", Arc::new(ParquetTransformer::new())),
        ];
        for (name, transformer) in formats {
            let path = dir.path().join(format!("out.{name}"));
            let sink = FileSink::create_truncating(&path, transformer, schema()).expect("sink");
            assert_eq!(
                sink.written.load(Ordering::SeqCst),
                sink.writer_base,
                "{name}: opening the writer wrote through the handle"
            );
            assert_eq!(
                std::fs::metadata(&path).expect("metadata").len(),
                0,
                "{name}: a header or magic number is held back until the first batch, which is \
                 why a failed first batch can be cut back to where it started"
            );
        }
    }

    #[tokio::test]
    async fn bytes_appended_between_open_and_the_first_batch_survive_its_retry() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("out.csv");
        let mut sink =
            FileSink::create(&path, Faulty::new(csv(true), &[0]), schema()).expect("sink");

        append_externally(&path, b"x\n");
        sink.write_batch(&batch(&[1]))
            .await
            .expect_err("the first batch fails after its header landed");
        assert_eq!(
            text(&path),
            "x\n",
            "the cut stops at the other process's bytes"
        );

        sink.write_batch(&batch(&[1])).await.expect("retry");
        sink.finish().await.expect("finish");
        assert_eq!(text(&path), "x\nid\n1\n");
    }

    #[tokio::test]
    async fn a_writer_that_writes_at_open_cannot_be_rebuilt_and_the_sink_refuses() {
        /// Writes its preamble through the handle when it opens, as a format
        /// that does not hold it back would.
        struct EagerPreamble;

        impl Transformer for EagerPreamble {
            fn format(&self) -> &'static str {
                "eager"
            }

            fn open_writer(
                &self,
                mut output: Box<dyn Write + Send>,
                _schema: Arc<Schema>,
            ) -> Result<Box<dyn BatchWriter>, SaciError> {
                output.write_all(b"MAGIC\n").expect("preamble");
                Ok(Box::new(EagerWriter { output }))
            }
        }

        struct EagerWriter {
            output: Box<dyn Write + Send>,
        }

        impl BatchWriter for EagerWriter {
            fn write_batch(&mut self, _batch: &RecordBatch) -> Result<(), SaciError> {
                self.output.write_all(b"row\n").expect("row");
                Err(SaciError::generic("injected write failure"))
            }

            fn finish(self: Box<Self>) -> Result<(), SaciError> {
                Ok(())
            }
        }

        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("out.txt");
        let mut sink = FileSink::create(&path, Arc::new(EagerPreamble), schema()).expect("sink");

        sink.write_batch(&batch(&[1])).await.expect_err("fails");
        assert_eq!(text(&path), "MAGIC\n", "only the failed batch was cut");
        let err = sink
            .write_batch(&batch(&[1]))
            .await
            .expect_err("a new writer would write the preamble again");
        assert!(err.message().contains("cannot be retried"), "got: {err}");
    }
}
