//! [`FileSource`]: a local file read through a codec.
//!
//! One dedicated OS thread drives the codec's
//! [`BatchReader`](crate::codec::BatchReader) and pushes batches down a
//! bounded channel; [`Source::next_batch`] awaits that channel. The executor
//! never blocks on the disk and the file is never materialised in full.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::codec::Codec;
use crate::connector::file::FileSourceConfig;
use crate::error::SaciError;
use crate::io::{CorruptRecord, Source};

/// Batches the reader thread may queue before it blocks. Four lets it stay
/// slightly ahead of the pipeline without holding much memory.
const CHANNEL_CAPACITY: usize = 4;

/// Local-file [`Source`]. The codec decodes; this type owns the file
/// handle, the reader thread and the channel.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
///
/// use saci::connector::file::{FileSource, FileSourceConfig};
/// use saci::codec::parquet::ParquetCodec;
///
/// // Parquet carries its own schema, so nothing is declared here.
/// let src = FileSource::new(
///     FileSourceConfig::new("trades.parquet"),
///     None,
///     Arc::new(ParquetCodec::new()),
/// )
/// .unwrap();
/// ```
pub struct FileSource {
    rx: mpsc::Receiver<Result<RecordBatch, SaciError>>,
    schema: Arc<Schema>,
    estimated_rows: Option<usize>,
    skip_corrupt: Arc<AtomicBool>,
    corrupt: Arc<Mutex<Vec<CorruptRecord>>>,
}

impl FileSource {
    /// Open `config.path`, hand the handle to `codec`, and spawn the reader
    /// thread. Returns once the format has reported its schema, which for a
    /// self-describing format means its metadata has been read.
    ///
    /// `schema` is the schema the config declared, `None` when it declared
    /// none. What that means is the format's business: csv requires one,
    /// parquet, avro and arrow-ipc project onto one, ndjson infers when it is
    /// absent.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the file cannot be opened, or the
    /// codec's own error when it cannot read this handle.
    pub fn new(
        config: FileSourceConfig,
        schema: Option<Arc<Schema>>,
        codec: Arc<dyn Codec>,
    ) -> Result<Self, SaciError> {
        Self::new_with_corrupt(config, schema, codec, false)
    }

    /// Same as [`new`](Self::new), configuring `skip_corrupt` before the reader
    /// thread starts polling the codec's [`BatchReader`](crate::codec::BatchReader).
    pub fn new_with_corrupt(
        config: FileSourceConfig,
        schema: Option<Arc<Schema>>,
        codec: Arc<dyn Codec>,
        skip_corrupt: bool,
    ) -> Result<Self, SaciError> {
        let path = config.path;
        let file = std::fs::File::open(&path)
            .map_err(|e| SaciError::generic(format!("FileSource: cannot open {path:?}: {e}")))?;
        let mut reader = codec.open_reader(file.into(), schema)?;
        reader.set_skip_corrupt(skip_corrupt);
        let schema = reader.schema();
        let estimated_rows = reader.estimated_rows();
        let skip_flag = Arc::new(AtomicBool::new(skip_corrupt));
        let skip_flag_bg = Arc::clone(&skip_flag);
        let corrupt = Arc::new(Mutex::new(Vec::new()));
        let corrupt_bg = Arc::clone(&corrupt);

        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        std::thread::spawn(move || {
            loop {
                reader.set_skip_corrupt(skip_flag_bg.load(Ordering::Acquire));
                let res = reader.next_batch();
                let drained = reader.drain_corrupt();
                if !drained.is_empty()
                    && let Ok(mut guard) = corrupt_bg.lock()
                {
                    guard.extend(drained);
                }
                match res {
                    Ok(None) => break,
                    Ok(Some(batch)) => {
                        // A send error means the source was dropped (pipeline
                        // aborted), so this thread has nowhere left to go.
                        if tx.blocking_send(Ok(batch)).is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.blocking_send(Err(e));
                        break;
                    }
                }
            }
        });

        Ok(Self {
            rx,
            schema,
            estimated_rows,
            skip_corrupt: skip_flag,
            corrupt,
        })
    }

    /// The same, with the open and the metadata read moved off the executor.
    ///
    /// Prefer this from async code: a Parquet footer read is disk IO on the
    /// calling thread otherwise.
    ///
    /// # Errors
    ///
    /// Returns what [`new`](Self::new) returns, or [`SaciError::Generic`] if
    /// the blocking task panics.
    pub async fn new_async(
        config: FileSourceConfig,
        schema: Option<Arc<Schema>>,
        codec: Arc<dyn Codec>,
    ) -> Result<Self, SaciError> {
        tokio::task::spawn_blocking(move || Self::new(config, schema, codec))
            .await
            .map_err(|e| SaciError::generic(format!("FileSource: spawn_blocking panic: {e}")))?
    }
}

#[async_trait]
impl Source for FileSource {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        match self.rx.recv().await {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.estimated_rows
    }

    fn set_skip_corrupt(&mut self, skip: bool) {
        self.skip_corrupt.store(skip, Ordering::Release);
    }

    fn drain_corrupt(&mut self) -> Vec<CorruptRecord> {
        self.corrupt
            .lock()
            .map(|mut g| std::mem::take(&mut *g))
            .unwrap_or_default()
    }
}
