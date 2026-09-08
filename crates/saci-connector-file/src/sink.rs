//! [`FileSink`]: a local file written through a transformer.

use std::path::Path;
use std::sync::Arc;

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
/// Every batch is synced to the file's directory entry before the next one
/// lands. Windows only flushes a held-open file's size metadata lazily, so
/// without the sync a long-running stream shows a 0-byte file until the sink
/// closes, and the last written batch is not yet durable.
pub struct FileSink {
    writer: Option<Box<dyn BatchWriter>>,
    /// Clone of the file handle, kept out of the transformer's reach so each
    /// batch can be synced after the writer's own buffer flush.
    sync: Option<std::fs::File>,
    schema: Arc<Schema>,
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
            std::fs::OpenOptions::new().create(true).append(true),
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

    /// Open `path` with `options` and wire the handle up: one clone for the
    /// per-batch sync, the original to the transformer's writer.
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
        let sync = file.try_clone().map_err(|e| {
            SaciError::generic(format!("FileSink: cannot clone handle for {path:?}: {e}"))
        })?;
        let writer = transformer.open_writer(Box::new(file), Arc::clone(&schema))?;
        Ok(Self {
            writer: Some(writer),
            sync: Some(sync),
            schema,
        })
    }
}

#[async_trait]
impl Sink for FileSink {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        self.writer
            .as_mut()
            .ok_or_else(|| SaciError::generic("FileSink: write_batch called after finish"))?
            .write_batch(batch)?;
        // The writer flushed its buffers; sync_data forces the file's size
        // metadata out of the OS cache too, so Explorer and `dir` show the
        // batch that just landed instead of the creation-time size.
        self.sync
            .as_mut()
            .ok_or_else(|| SaciError::generic("FileSink: write_batch called after finish"))?
            .sync_data()
            .map_err(|e| SaciError::generic(format!("FileSink: sync failed: {e}")))
    }

    async fn finish(&mut self) -> Result<(), SaciError> {
        // Idempotent: a second call has no writer left and nothing to flush.
        match self.writer.take() {
            None => Ok(()),
            Some(writer) => {
                writer.finish()?;
                // The footer (parquet) just reached the OS; make it durable too,
                // then release the sync handle.
                if let Some(sync) = self.sync.take() {
                    sync.sync_data()
                        .map_err(|e| SaciError::generic(format!("FileSink: sync failed: {e}")))?;
                }
                Ok(())
            }
        }
    }
}
