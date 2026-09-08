//! [`RedbSource`]: the entries of one redb table, read through a transformer.
//!
//! The source lists the table's matching keys once, then walks that list one
//! entry at a time. Each value is spooled into an unnamed tempfile, one
//! dedicated OS thread drives the transformer's
//! [`BatchReader`](saci_transformer::BatchReader) over that file, and
//! [`Source::next_batch`] awaits the bounded channel the thread feeds. EOF is
//! the key list exhausted; there is no re-scan or tail mode, so this is a
//! finite source every run mode can drive.

use std::collections::VecDeque;
use std::io::{Seek, Write};
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use redb::{Database, ReadOnlyDatabase, ReadableDatabase, TableDefinition, TableError};
use tokio::sync::mpsc;

use saci_core::error::SaciError;
use saci_core::io::source::Source;
use saci_transformer::Transformer;

use crate::config::RedbSourceConfig;

/// Batches the reader thread may queue before it blocks. Matches
/// `saci-connector-file`'s.
const CHANNEL_CAPACITY: usize = 4;

/// redb [`Source`]. The transformer decodes; this type owns the database
/// handle, the key list, the spool file, the reader thread and the channel.
///
/// # Example
///
/// ```rust,no_run
/// use std::path::PathBuf;
/// use std::sync::Arc;
///
/// use arrow_schema::Schema;
/// use saci_connector_redb::{RedbSource, RedbSourceConfig};
/// use saci_transformer_csv::CsvTransformer;
///
/// let config = RedbSourceConfig {
///     directory: PathBuf::from("/data/orders"),
///     file: "orders.redb".to_string(),
///     table: "records".to_string(),
///     key_prefix: "orders/".to_string(),
///     key_suffix: ".csv".to_string(),
///     check_integrity: false,
///     cache_size_bytes: None,
///     consume: false,
///     schema_fields: Vec::new(),
/// };
/// let src = RedbSource::new(
///     config,
///     Arc::new(Schema::empty()),
///     Arc::new(CsvTransformer::new(true)),
/// )
/// .unwrap();
/// ```
pub struct RedbSource {
    path: PathBuf,
    table: String,
    key_prefix: String,
    key_suffix: String,
    check_integrity: bool,
    cache_size_bytes: Option<usize>,
    transformer: Arc<dyn Transformer>,
    declared: Arc<Schema>,
    /// `None` before the first batch opens the file and again at EOF.
    ///
    /// A [`ReadOnlyDatabase`](redb::ReadOnlyDatabase) rather than a
    /// `Database`: redb opens it `read(true)` and takes a **shared** OS lock,
    /// so several sources may read one file at once and a read-only file or
    /// filesystem still works. It also never repairs, so a source cannot
    /// mutate the file it is reading.
    ///
    /// No mutex: every use after the open is `begin_read(&self)`.
    db: Option<Arc<ReadOnlyDatabase>>,
    /// `None` until the first `next_batch` scans the table.
    pending: Option<VecDeque<String>>,
    /// Batches from the entry currently being decoded.
    current: Option<mpsc::Receiver<Result<RecordBatch, SaciError>>>,
    /// Delete what this instance yielded at [`Source::finish`].
    consume: bool,
    /// The keys whose entries were handed over whole, oldest first, waiting
    /// for [`Source::finish`] to delete them. Empty unless `consume`.
    yielded: Vec<String>,
}

impl RedbSource {
    /// Prepare the source without touching the disk.
    ///
    /// The file is opened by the first [`Source::next_batch`], so
    /// `saci-service validate` passes while the file is absent or held by a
    /// sink, matching `S3Source` and `HttpSource`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when the config is invalid. No file
    /// is opened.
    pub fn new(
        config: RedbSourceConfig,
        schema: Arc<Schema>,
        transformer: Arc<dyn Transformer>,
    ) -> Result<Self, SaciError> {
        config.validate("RedbSource")?;
        Ok(Self {
            path: config.path(),
            table: config.table,
            key_prefix: config.key_prefix,
            key_suffix: config.key_suffix,
            check_integrity: config.check_integrity,
            cache_size_bytes: config.cache_size_bytes,
            transformer,
            declared: schema,
            db: None,
            pending: None,
            current: None,
            consume: config.consume,
            yielded: Vec::new(),
        })
    }

    /// Open the file and collect the keys this source is to read, in key
    /// order, which is the order the sink wrote them in.
    async fn scan(&mut self) -> Result<(), SaciError> {
        let path = self.path.clone();
        let table = self.table.clone();
        let prefix = self.key_prefix.clone();
        let suffix = self.key_suffix.clone();
        let check = self.check_integrity;
        let cache = self.cache_size_bytes;
        let (db, keys) = tokio::task::spawn_blocking(move || {
            if check {
                check_integrity(&path, cache)?;
            }
            let mut builder = Database::builder();
            if let Some(bytes) = cache {
                builder.set_cache_size(bytes);
            }
            // An absent file is an error, not EOF: a config naming a file that
            // is not there is a mistake worth reporting. A sink holding the
            // same file arrives here too, because its lock is exclusive.
            let db = builder.open_read_only(&path).map_err(|e| match e {
                // A read-only open never repairs, so a file whose last write
                // left no allocator state is refused rather than rewritten.
                // `Database::drop` writes that state, and so does any commit
                // under two-phase commit, which `quick_repair` forces on; a
                // file reaches this state only when a sink ran with both
                // knobs off and its process died before `finish`.
                redb::DatabaseError::RepairAborted => SaciError::generic(format!(
                    "RedbSource: {} was not shut down cleanly and a read-only open \
                     cannot repair it; set 'check_integrity' on this source, or run \
                     a RedbSink over the file once, to repair it read-write",
                    path.display()
                )),
                e => SaciError::generic(format!("RedbSource: cannot open {}: {e}", path.display())),
            })?;
            let keys = collect_keys(&db, &table, &prefix, &suffix, &path)?;
            Ok::<_, SaciError>((db, keys))
        })
        .await
        .map_err(|e| SaciError::generic(format!("RedbSource: spawn_blocking panic: {e}")))??;
        self.db = Some(Arc::new(db));
        self.pending = Some(keys.into());
        Ok(())
    }

    /// Read one entry's bytes and hand the transformer a spool file over them.
    async fn open_entry(
        &self,
        key: String,
    ) -> Result<Box<dyn saci_transformer::BatchReader>, SaciError> {
        let db = Arc::clone(self.db.as_ref().expect("the file was just opened"));
        let path = self.path.clone();
        let table = self.table.clone();
        let transformer = Arc::clone(&self.transformer);
        let declared = Arc::clone(&self.declared);
        tokio::task::spawn_blocking(move || {
            let definition: TableDefinition<&str, &[u8]> = TableDefinition::new(&table);
            let txn = db.begin_read().map_err(|e| {
                SaciError::generic(format!("RedbSource: begin_read on {}: {e}", path.display()))
            })?;
            let open = txn.open_table(definition).map_err(|e| {
                SaciError::generic(format!(
                    "RedbSource: cannot open table '{table}' in {}: {e}",
                    path.display()
                ))
            })?;
            let value = open
                .get(key.as_str())
                .map_err(|e| {
                    SaciError::generic(format!(
                        "RedbSource: get '{key}' from table '{table}' in {}: {e}",
                        path.display()
                    ))
                })?
                .ok_or_else(|| {
                    SaciError::generic(format!(
                        "RedbSource: entry '{key}' vanished from table '{table}' in {}",
                        path.display()
                    ))
                })?;
            let bytes = value.value();
            let size = bytes.len();
            // A spool file rather than a `Vec`: `Transformer::open_reader`
            // takes a concrete `std::fs::File`, and the OS reclaims an unnamed
            // tempfile on close.
            let mut spool = tempfile::tempfile()
                .map_err(|e| SaciError::generic(format!("RedbSource: spool file: {e}")))?;
            spool
                .write_all(bytes)
                .map_err(|e| SaciError::generic(format!("RedbSource: spool write: {e}")))?;
            spool
                .rewind()
                .map_err(|e| SaciError::generic(format!("RedbSource: spool rewind: {e}")))?;

            #[cfg(feature = "tracing")]
            tracing::info!(key = %key, bytes = size, "RedbSource: entry spooled");
            #[cfg(not(feature = "tracing"))]
            let _ = size;

            transformer.open_reader(spool, Some(declared))
        })
        .await
        .map_err(|e| SaciError::generic(format!("RedbSource: spawn_blocking panic: {e}")))?
    }
}

/// Walk the whole file and refuse a corrupted one.
///
/// Opens the file read-write for the duration: `Database::check_integrity`
/// takes `&mut self` and may repair, which is inherently a write. The handle
/// is dropped before the caller takes its shared read-only lock, so the
/// exclusive window lasts only as long as the check, and a `check_integrity`
/// source therefore needs a writable file.
fn check_integrity(path: &std::path::Path, cache: Option<usize>) -> Result<(), SaciError> {
    let mut builder = Database::builder();
    if let Some(bytes) = cache {
        builder.set_cache_size(bytes);
    }
    let mut db = builder.open(path).map_err(|e| {
        SaciError::generic(format!(
            "RedbSource: cannot open {} for the integrity check: {e}",
            path.display()
        ))
    })?;
    match db.check_integrity() {
        Ok(true) => {}
        Ok(false) => {
            #[cfg(feature = "tracing")]
            tracing::warn!(
                path = %path.display(),
                "RedbSource: the file was repaired during the integrity check"
            );
        }
        Err(e) => {
            return Err(SaciError::generic(format!(
                "RedbSource: integrity check of {} failed: {e}",
                path.display()
            )));
        }
    }
    Ok(())
}

/// The keys of `table` that carry both `prefix` and `suffix`, in key order.
///
/// The segment between the two is never parsed, so an entry another writer put
/// in the same table under a matching name reaches the transformer and fails
/// there by name rather than being skipped in silence.
fn collect_keys(
    db: &ReadOnlyDatabase,
    table: &str,
    prefix: &str,
    suffix: &str,
    path: &std::path::Path,
) -> Result<Vec<String>, SaciError> {
    let definition: TableDefinition<&str, &[u8]> = TableDefinition::new(table);
    let txn = db.begin_read().map_err(|e| {
        SaciError::generic(format!("RedbSource: begin_read on {}: {e}", path.display()))
    })?;
    let open = match txn.open_table(definition) {
        Ok(t) => t,
        // A file a sink created but never wrote to holds no table; that is an
        // empty source, not a failure.
        Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(e) => {
            return Err(SaciError::generic(format!(
                "RedbSource: cannot open table '{table}' in {}: {e}",
                path.display()
            )));
        }
    };
    let range = open.range(prefix..).map_err(|e| {
        SaciError::generic(format!(
            "RedbSource: scan table '{table}' in {}: {e}",
            path.display()
        ))
    })?;
    let mut keys = Vec::new();
    for entry in range {
        let (guard, _) = entry.map_err(|e| {
            SaciError::generic(format!(
                "RedbSource: scan table '{table}' in {}: {e}",
                path.display()
            ))
        })?;
        let key = guard.value();
        // The range is ordered, so the first key past the prefix ends it.
        if !key.starts_with(prefix) {
            break;
        }
        if key.ends_with(suffix) {
            keys.push(key.to_string());
        }
    }
    Ok(keys)
}

#[async_trait]
impl Source for RedbSource {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.declared)
    }

    /// One batch off the entry at the front of the key list, or `Ok(None)`
    /// once that list is exhausted. The first call opens the file and scans
    /// the table.
    ///
    /// # Errors
    ///
    /// A key leaves the list only when its entry's batch stream ends cleanly,
    /// so every other outcome leaves it at the front: a scan error, a read or
    /// spool error, a reader the entry's bytes refuse, and a decode error
    /// raised part way through the entry. A caller's retry therefore
    /// re-attempts that same entry rather than advancing past it, which
    /// replays the batches of that entry it had already handed over. That
    /// duplication is what this engine's at-least-once delivery allows.
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        // 1. First call: open the file and scan the table once.
        if self.pending.is_none() {
            self.scan().await?;
        }

        loop {
            // 2. Drain the entry currently being decoded before opening the
            //    next one. Only a clean end of that entry's stream (`None`)
            //    pops its key.
            if let Some(rx) = &mut self.current {
                match rx.recv().await {
                    Some(Ok(batch)) => return Ok(Some(batch)),
                    Some(Err(e)) => {
                        self.current = None;
                        return Err(e);
                    }
                    None => {
                        self.current = None;
                        let done = self
                            .pending
                            .as_mut()
                            .expect("pending was just set")
                            .pop_front();
                        // The entry was handed over whole, so `finish` may
                        // delete it. A key that left the list any other way
                        // never reaches this arm.
                        if self.consume
                            && let Some(key) = done
                        {
                            self.yielded.push(key);
                        }
                    }
                }
            }

            // 3. Next entry, or EOF once the list is exhausted. Peeked, not
            //    popped: opening or decoding it below may still fail, and step
            //    2 above is the only place that removes a key.
            let Some(key) = self
                .pending
                .as_ref()
                .expect("pending was just set")
                .front()
                .cloned()
            else {
                // Dropping the last handle releases the shared read lock, so
                // a sink may take the file once the source is done.
                self.db = None;
                return Ok(None);
            };

            // 4. Spool the entry and open the reader off the executor.
            let mut reader = self.open_entry(key).await?;

            // 5. Decode off the executor, feeding a bounded channel; the
            //    executor never blocks on the disk. The loop above drains the
            //    receiver the entry just opened.
            let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
            std::thread::spawn(move || {
                loop {
                    match reader.next_batch() {
                        Ok(None) => break,
                        Ok(Some(batch)) => {
                            // A send error means the source was dropped
                            // (pipeline aborted), so this thread has nowhere
                            // left to go.
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
            self.current = Some(rx);
        }
    }

    /// Delete the entries this instance yielded, when `consume` is set.
    ///
    /// Nothing to delete, or `consume` off, and this is a no-op. Otherwise
    /// the read-only handle is dropped and the file re-opened read-write for
    /// one delete transaction, because redb's read-write open is exclusive.
    /// A caller that stopped part way through the file keeps whatever entry
    /// was still being decoded: only a stream that ended records its key.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the open, the table, a remove or
    /// the commit fails. The yielded keys are kept on an error, so a later
    /// `finish` retries the delete; an instance dropped instead deletes
    /// nothing and its entries are delivered again.
    async fn finish(&mut self) -> Result<(), SaciError> {
        if self.yielded.is_empty() {
            return Ok(());
        }
        // Both handles go before the exclusive write open. The decode thread
        // holds only its spool file and the sender half, so dropping the
        // receiver is what ends it; the database `Arc` never left the
        // `spawn_blocking` that read the entry, so this is its last handle.
        self.current = None;
        self.db = None;
        let path = self.path.clone();
        let table = self.table.clone();
        let cache = self.cache_size_bytes;
        let keys = self.yielded.clone();
        tokio::task::spawn_blocking(move || {
            let fail = |e: &dyn std::fmt::Display| {
                SaciError::generic(format!(
                    "RedbSource: consuming {} entries of table '{table}' in {}: {e}",
                    keys.len(),
                    path.display()
                ))
            };
            let definition: TableDefinition<&str, &[u8]> = TableDefinition::new(&table);
            let mut builder = Database::builder();
            if let Some(bytes) = cache {
                builder.set_cache_size(bytes);
            }
            let db = builder.open(&path).map_err(|e| fail(&e))?;
            let txn = db.begin_write().map_err(|e| fail(&e))?;
            {
                let mut open = txn.open_table(definition).map_err(|e| fail(&e))?;
                for key in &keys {
                    open.remove(key.as_str()).map_err(|e| fail(&e))?;
                }
            }
            txn.commit().map_err(|e| fail(&e))
        })
        .await
        .map_err(|e| SaciError::generic(format!("RedbSource: spawn_blocking panic: {e}")))??;
        self.yielded.clear();
        Ok(())
    }
}
