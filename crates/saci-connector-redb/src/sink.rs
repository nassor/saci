//! [`RedbSink`]: one committed redb entry per batch.
//!
//! Each batch is encoded into a self-contained document by the node's
//! transformer and stored under a generated key, in its own write transaction.
//! Nothing is buffered between batches: an accepted batch is already on disk
//! (with the default `Immediate` durability), so a failed run leaves every
//! entry it reported as written behind.
//!
//! [`Sink::finish`] drops the database, which releases redb's exclusive file
//! lock, so a source can read the file back afterwards in the same process.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use redb::{Database, ReadableDatabase, TableDefinition, TableError};

use saci_core::error::SaciError;
use saci_core::io::sink::Sink;
use saci_transformer::Transformer;

use crate::config::RedbSinkConfig;
use crate::key::{entry_key, parse_seq};

/// A `std::io::Write` handle onto a shared byte buffer.
///
/// [`Transformer::open_writer`] takes ownership of the handle and
/// [`BatchWriter::finish`](saci_transformer::BatchWriter::finish) consumes the
/// writer, so the sink keeps a second handle on the same bytes to read the
/// finished document back out. The same trick `S3Sink` and `HttpSink` play,
/// and `FileSink` with `File::try_clone`.
#[derive(Clone, Default)]
struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

impl SharedBuffer {
    /// Drain the buffer, leaving it empty.
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

/// redb [`Sink`]: one entry per batch, the transformer writes the value.
///
/// # Example
///
/// ```rust,no_run
/// use std::path::PathBuf;
/// use std::sync::Arc;
///
/// use arrow_schema::{DataType, Field, Schema};
/// use saci_connector_redb::{DurabilityMode, RedbSink, RedbSinkConfig};
/// use saci_transformer_csv::CsvTransformer;
///
/// let config = RedbSinkConfig {
///     directory: PathBuf::from("/data/orders"),
///     file: "orders.redb".to_string(),
///     table: "records".to_string(),
///     key_prefix: "orders/".to_string(),
///     key_suffix: ".csv".to_string(),
///     check_integrity: false,
///     cache_size_bytes: None,
///     compact: true,
///     durability: DurabilityMode::Immediate,
///     two_phase_commit: true,
///     quick_repair: true,
///     schema_fields: Vec::new(),
/// };
/// let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
/// let sink = RedbSink::open(config, schema, Arc::new(CsvTransformer::new(true))).unwrap();
/// ```
pub struct RedbSink {
    /// `None` after [`Sink::finish`], which is what releases the file lock.
    ///
    /// Behind a mutex because [`Database::compact`] needs `&mut Database`, and
    /// the transaction closures run on a blocking thread that owns a clone.
    db: Option<Arc<Mutex<Database>>>,
    path: std::path::PathBuf,
    table: String,
    key_prefix: String,
    key_suffix: String,
    /// The key the next accepted batch takes. Advanced only after a commit
    /// succeeds, so a retried batch reuses the key the failed one did not take.
    next_seq: u64,
    durability: redb::Durability,
    two_phase_commit: bool,
    quick_repair: bool,
    compact: bool,
    schema: Arc<Schema>,
    transformer: Arc<dyn Transformer>,
}

impl RedbSink {
    /// Create or open the redb file and resume the key sequence from it.
    ///
    /// Synchronous and touches the disk, matching `FileSink::create`: the
    /// directory is created if absent, the file is created if absent, and the
    /// highest key already carrying `key_prefix`/`key_suffix` decides where the
    /// sequence continues.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when the config is invalid, and
    /// [`SaciError::Generic`] when the directory cannot be created, the file
    /// cannot be opened (a second open of a file this process already holds is
    /// one such failure: redb locks it exclusively), the integrity check fails,
    /// or the table cannot be read.
    pub fn open(
        config: RedbSinkConfig,
        schema: Arc<Schema>,
        transformer: Arc<dyn Transformer>,
    ) -> Result<Self, SaciError> {
        config.validate("RedbSink")?;
        let path = config.path();
        std::fs::create_dir_all(&config.directory).map_err(|e| {
            SaciError::generic(format!(
                "RedbSink: cannot create directory {}: {e}",
                config.directory.display()
            ))
        })?;
        let mut builder = Database::builder();
        if let Some(bytes) = config.cache_size_bytes {
            builder.set_cache_size(bytes);
        }
        let mut db = builder.create(&path).map_err(|e| {
            SaciError::generic(format!("RedbSink: cannot open {}: {e}", path.display()))
        })?;
        if config.check_integrity {
            check_integrity("RedbSink", &mut db, &path)?;
        }
        let next_seq = resume_seq(&db, &config.table, &config.key_prefix, &config.key_suffix)
            .map_err(|e| {
                SaciError::generic(format!(
                    "RedbSink: cannot open table '{}' in {}: {e}",
                    config.table,
                    path.display()
                ))
            })?;
        Ok(Self {
            db: Some(Arc::new(Mutex::new(db))),
            path,
            table: config.table,
            key_prefix: config.key_prefix,
            key_suffix: config.key_suffix,
            next_seq,
            durability: config.durability.into(),
            two_phase_commit: config.two_phase_commit,
            quick_repair: config.quick_repair,
            compact: config.compact,
            schema,
            transformer,
        })
    }

    /// Encode `batch` into one self-contained document.
    ///
    /// Called inline, exactly as `FileSink`, `S3Sink` and `HttpSink` call their
    /// encoders: the format is CPU-bound, not IO-bound, so a blocking thread
    /// buys nothing here. The commit below is the part that fsyncs.
    fn encode(&self, batch: &RecordBatch) -> Result<Vec<u8>, SaciError> {
        let buffer = SharedBuffer::default();
        let mut writer = self
            .transformer
            .open_writer(Box::new(buffer.clone()), Arc::clone(&self.schema))?;
        writer.write_batch(batch)?;
        // Consumes the writer, so the format's own trailer reaches the buffer.
        writer.finish()?;
        Ok(buffer.take())
    }
}

/// Walk the whole file and refuse a corrupted one.
fn check_integrity(what: &str, db: &mut Database, path: &std::path::Path) -> Result<(), SaciError> {
    match db.check_integrity() {
        Ok(true) => {}
        Ok(false) => {
            #[cfg(feature = "tracing")]
            tracing::warn!(
                path = %path.display(),
                "{what}: the file was repaired during the integrity check"
            );
            #[cfg(not(feature = "tracing"))]
            let _ = (what, path);
        }
        Err(e) => {
            return Err(SaciError::generic(format!(
                "{what}: integrity check of {} failed: {e}",
                path.display()
            )));
        }
    }
    Ok(())
}

/// The sequence number the next entry takes.
///
/// The highest key that carries both `prefix` and `suffix` and a well-formed
/// sequence segment, plus one; `0` when the table holds no such key, and `0`
/// when the table does not exist yet, which is what a file the first write
/// created looks like.
fn resume_seq(db: &Database, table: &str, prefix: &str, suffix: &str) -> Result<u64, SaciError> {
    let definition: TableDefinition<&str, &[u8]> = TableDefinition::new(table);
    let txn = db
        .begin_read()
        .map_err(|e| SaciError::generic(format!("begin_read: {e}")))?;
    let table = match txn.open_table(definition) {
        Ok(t) => t,
        // The table is created by the first write, so an absent one is an
        // empty sequence rather than a failure.
        Err(TableError::TableDoesNotExist(_)) => return Ok(0),
        Err(e) => return Err(SaciError::generic(e.to_string())),
    };
    // Descending from the end of the range: the first key that parses is the
    // highest one this connector wrote.
    //
    // Every key that does not start with `prefix` but sorts above it also
    // sorts above every key that does (they differ inside the prefix, at a
    // byte that is greater), so in reverse those foreign keys all come first.
    // Skipping them rather than stopping is what keeps a second writer's keys
    // in the same table from resetting this sequence to zero and colliding on
    // the first write.
    let range = table
        .range(prefix..)
        .map_err(|e| SaciError::generic(format!("range: {e}")))?;
    for entry in range.rev() {
        let (key, _) = entry.map_err(|e| SaciError::generic(format!("range: {e}")))?;
        if let Some(seq) = parse_seq(key.value(), prefix, suffix) {
            return Ok(seq.saturating_add(1));
        }
    }
    Ok(0)
}

/// Lock the database, adopting a poisoned guard: a panicking transaction
/// leaves redb's own state consistent, so there is nothing to protect against.
fn lock(db: &Mutex<Database>) -> MutexGuard<'_, Database> {
    db.lock().unwrap_or_else(PoisonError::into_inner)
}

#[async_trait]
impl Sink for RedbSink {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    /// Encode `batch` and commit it as one entry.
    ///
    /// A batch with no rows writes nothing: an empty document is not an entry
    /// a source would gain anything from reading back, and `S3Sink` skips a
    /// writer that took no rows for the same reason.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] after `finish`, when the transformer
    /// refuses the batch, when the generated key is already present (a foreign
    /// writer took it), or on any redb failure. The sequence advances only
    /// after the commit succeeds, so a caller's retry reuses the same key.
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        let Some(db) = self.db.as_ref() else {
            return Err(SaciError::generic(
                "RedbSink: write_batch called after finish",
            ));
        };
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let bytes = self.encode(batch)?;
        let key = entry_key(&self.key_prefix, self.next_seq, &self.key_suffix);

        let db = Arc::clone(db);
        let table = self.table.clone();
        let path = self.path.clone();
        let durability = self.durability;
        let two_phase_commit = self.two_phase_commit;
        let quick_repair = self.quick_repair;
        let commit_key = key.clone();
        let size = bytes.len();
        tokio::task::spawn_blocking(move || {
            let definition: TableDefinition<&str, &[u8]> = TableDefinition::new(&table);
            let db = lock(&db);
            let mut txn = db.begin_write().map_err(|e| {
                SaciError::generic(format!("RedbSink: begin_write on {}: {e}", path.display()))
            })?;
            txn.set_durability(durability).map_err(|e| {
                SaciError::generic(format!("RedbSink: durability on {}: {e}", path.display()))
            })?;
            txn.set_two_phase_commit(two_phase_commit);
            txn.set_quick_repair(quick_repair);
            {
                let mut open = txn.open_table(definition).map_err(|e| {
                    SaciError::generic(format!(
                        "RedbSink: cannot open table '{table}' in {}: {e}",
                        path.display()
                    ))
                })?;
                let previous = open
                    .insert(commit_key.as_str(), bytes.as_slice())
                    .map_err(|e| {
                        SaciError::generic(format!(
                            "RedbSink: insert '{commit_key}' into table '{table}' of {}: {e}",
                            path.display()
                        ))
                    })?;
                if previous.is_some() {
                    // Dropping the table and then the transaction aborts it,
                    // so the value already stored under this key stays.
                    return Err(SaciError::generic(format!(
                        "RedbSink: key '{commit_key}' already exists in table '{table}' of {}",
                        path.display()
                    )));
                }
            }
            txn.commit().map_err(|e| {
                SaciError::generic(format!("RedbSink: commit to {}: {e}", path.display()))
            })
        })
        .await
        .map_err(|e| SaciError::generic(format!("RedbSink: spawn_blocking panic: {e}")))??;

        self.next_seq += 1;
        #[cfg(feature = "tracing")]
        tracing::info!(
            key = %key,
            rows = batch.num_rows(),
            bytes = size,
            "RedbSink: entry committed"
        );
        #[cfg(not(feature = "tracing"))]
        let _ = (key, size);
        Ok(())
    }

    /// Make every commit durable, optionally compact, and release the file.
    ///
    /// Idempotent: a second call has no database left to close and returns
    /// `Ok(())`. The blocking closure owns the last handle on the database, so
    /// the file lock is gone by the time this returns and another `RedbSink` or
    /// [`RedbSource`](crate::RedbSource) may open the same path.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the final durable commit or the
    /// compaction fails.
    async fn finish(&mut self) -> Result<(), SaciError> {
        let Some(db) = self.db.take() else {
            return Ok(());
        };
        let path = self.path.clone();
        let durability = self.durability;
        let compact = self.compact;
        tokio::task::spawn_blocking(move || {
            let mut db = lock(&db);
            if matches!(durability, redb::Durability::None) {
                // Every earlier commit is still only in the page cache; one
                // empty Immediate commit is what redb documents as the way to
                // make them durable.
                let mut txn = db.begin_write().map_err(|e| {
                    SaciError::generic(format!("RedbSink: begin_write on {}: {e}", path.display()))
                })?;
                txn.set_durability(redb::Durability::Immediate)
                    .map_err(|e| {
                        SaciError::generic(format!(
                            "RedbSink: durability on {}: {e}",
                            path.display()
                        ))
                    })?;
                txn.commit().map_err(|e| {
                    SaciError::generic(format!("RedbSink: final commit to {}: {e}", path.display()))
                })?;
            }
            if compact {
                let compacted = db.compact().map_err(|e| {
                    SaciError::generic(format!(
                        "RedbSink: compaction of {} failed: {e}",
                        path.display()
                    ))
                })?;
                #[cfg(feature = "tracing")]
                tracing::info!(
                    compacted,
                    path = %path.display(),
                    "RedbSink: compacted"
                );
                #[cfg(not(feature = "tracing"))]
                let _ = compacted;
            }
            Ok::<_, SaciError>(())
        })
        .await
        .map_err(|e| SaciError::generic(format!("RedbSink: spawn_blocking panic: {e}")))?
    }
}
