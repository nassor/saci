//! [`FileSource`]: a local file read through a transformer.
//!
//! One dedicated OS thread drives the transformer's
//! [`BatchReader`](saci_transformer::BatchReader) and pushes batches down a
//! bounded channel; [`Source::next_batch`] awaits that channel. The executor
//! never blocks on the disk and the file is never materialised in full.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use tokio::sync::mpsc;

use saci_core::error::SaciError;
use saci_core::io::source::Source;
use saci_transformer::Transformer;

/// Batches the reader thread may queue before it blocks. Four lets it stay
/// slightly ahead of the pipeline without holding much memory.
const CHANNEL_CAPACITY: usize = 4;

/// Local-file [`Source`]. The transformer decodes; this type owns the file
/// handle, the reader thread and the channel.
///
/// # Example
///
/// ```rust,no_run
/// use std::path::Path;
/// use std::sync::Arc;
///
/// use saci_connector_file::FileSource;
/// use saci_transformer_parquet::ParquetTransformer;
///
/// // Parquet carries its own schema, so nothing is declared here.
/// let src = FileSource::open(
///     Path::new("trades.parquet"),
///     Arc::new(ParquetTransformer::new()),
///     None,
/// )
/// .unwrap();
/// ```
pub struct FileSource {
    rx: mpsc::Receiver<Result<RecordBatch, SaciError>>,
    schema: Arc<Schema>,
    estimated_rows: Option<usize>,
}

impl FileSource {
    /// Open `path`, hand the handle to `transformer`, and spawn the reader
    /// thread. Returns once the format has reported its schema, which for a
    /// self-describing format means its metadata has been read.
    ///
    /// `declared` is the schema the config named, `None` when it named none.
    /// What that means is the format's business: csv requires one, parquet,
    /// avro and arrow-ipc project onto one, ndjson infers when it is absent.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the file cannot be opened, or the
    /// transformer's own error when it cannot read this handle.
    pub fn open(
        path: &Path,
        transformer: Arc<dyn Transformer>,
        declared: Option<Arc<Schema>>,
    ) -> Result<Self, SaciError> {
        let file = std::fs::File::open(path)
            .map_err(|e| SaciError::generic(format!("FileSource: cannot open {path:?}: {e}")))?;
        let mut reader = transformer.open_reader(file, declared)?;
        let schema = reader.schema();
        let estimated_rows = reader.estimated_rows();

        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        std::thread::spawn(move || {
            loop {
                match reader.next_batch() {
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
        })
    }

    /// The same, with the open and the metadata read moved off the executor.
    ///
    /// Prefer this from async code: a Parquet footer read is disk IO on the
    /// calling thread otherwise.
    ///
    /// # Errors
    ///
    /// Returns what [`open`](Self::open) returns, or [`SaciError::Generic`] if
    /// the blocking task panics.
    pub async fn open_async(
        path: &Path,
        transformer: Arc<dyn Transformer>,
        declared: Option<Arc<Schema>>,
    ) -> Result<Self, SaciError> {
        let path: PathBuf = path.to_owned();
        tokio::task::spawn_blocking(move || Self::open(&path, transformer, declared))
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
}
