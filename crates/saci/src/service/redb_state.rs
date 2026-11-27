//! Persistent state client for the service runners: config files, processor
//! priors, and source cursors.
//!
//! One local, unreplicated redb file, declared by a `store "redb"` block. It
//! is standalone/stream-mode persistence: cluster mode has no `store` block,
//! because its application state is the raft-replicated `cluster-app.redb`
//! under `node.data_dir`.
//!
//! Three tables, all `&str -> &[u8]`:
//!
//! - `crate::config`: config name to the raw (pre-substitution) KDL bytes.
//! - `saci_priors`: `"{workflow_id}/{node_id}"` to a runtime's opaque state blob.
//! - `saci_cursors`: `"{workflow_id}/{source_id}"` to a postcard-encoded
//!   [`SourceCursorMeta`].
//!
//! There is no no-op twin: `service` implies `distributed`, which supplies
//! redb, so the real implementation is always compiled and the only switch a
//! caller needs is `Option<Arc<RedbStateClient>>`, which is `None` when the
//! config declares no store.
//!
//! Every method runs its transaction inside `spawn_blocking`, because a redb
//! commit fsyncs.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::SaciResult;
use crate::error::SaciError;
use crate::inspector_wire::{CursorResetReport, CursorSnapshot, WorkflowCursorsReport};

/// Config name to the raw KDL bytes it was loaded from.
const CONFIG_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("saci_config");
/// `"{workflow_id}/{node_id}"` to a runtime's opaque state blob.
const PRIORS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("saci_priors");
/// `"{workflow_id}/{source_id}"` to a postcard-encoded [`SourceCursorMeta`].
const CURSORS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("saci_cursors");
/// `"{workflow_id}/{source_id}/{ts_ms:020}"` to a historical postcard-encoded [`SourceCursorMeta`].
const CURSOR_HISTORY_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("saci_cursor_history");
/// `"{workflow_id}/{node_id}/{ts_ms:020}"` to a historical processor state blob.
const PRIOR_HISTORY_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("saci_prior_history");

/// Maximum retained historical snapshots per `(workflow_id, node_id)`.
const MAX_HISTORY_PER_NODE: usize = 64;

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// How many items a source delivered and when, persisted per workflow/source
/// so a restarted service can resume from its last save point.
///
/// Stream-mode sources are at-least-once: a missed cursor write costs one
/// replay, never a lost item.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceCursorMeta {
    /// Number of batches this source delivered since the workflow ran. One
    /// per arrival, however many credit-sized chunks the runner cut it into.
    pub items_processed: u64,
    /// Unix milliseconds of the last cursor write.
    pub last_batch_at_ms: u64,
}

/// Handle to the local redb state file declared by `store "redb"`.
pub struct RedbStateClient {
    db: Arc<Mutex<Database>>,
}

impl RedbStateClient {
    /// Open (creating if absent) the redb file at `path`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Store`] if the file cannot be created or opened.
    pub fn open(path: &Path) -> SaciResult<Self> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|e| {
                SaciError::store(format!("create store directory {}: {e}", parent.display()))
            })?;
        }
        let db = Database::create(path)
            .map_err(|e| SaciError::store(format!("open redb at {}: {e}", path.display())))?;
        Ok(Self {
            db: Arc::new(Mutex::new(db)),
        })
    }

    /// Persist the raw (pre env-substitution) KDL bytes of a config file.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Store`] on a redb failure.
    pub async fn put_config(&self, name: &str, kdl: &[u8]) -> SaciResult<()> {
        self.put(CONFIG_TABLE, name.to_string(), kdl.to_vec(), "config")
            .await
    }

    /// Load a processor's persisted state blob, if any.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Store`] on a redb failure.
    pub async fn load_prior(
        &self,
        workflow_id: &str,
        node_id: &str,
    ) -> SaciResult<Option<Vec<u8>>> {
        self.get(PRIORS_TABLE, join(workflow_id, node_id), "prior")
            .await
    }

    /// Persist a processor's state blob and record a timestamped history entry.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Store`] on a redb failure.
    pub async fn save_prior(
        &self,
        workflow_id: &str,
        node_id: &str,
        blob: &[u8],
    ) -> SaciResult<()> {
        let db = Arc::clone(&self.db);
        let key = join(workflow_id, node_id);
        let hist_prefix = format!("{key}/");
        let hist_key = format!("{hist_prefix}{:020}", now_unix_ms());
        let value = blob.to_vec();
        blocking("prior", move || {
            let db = lock(&db)?;
            let txn = db
                .begin_write()
                .map_err(|e| SaciError::store(format!("redb begin_write: {e}")))?;
            {
                let mut table = txn
                    .open_table(PRIORS_TABLE)
                    .map_err(|e| SaciError::store(format!("redb open prior table: {e}")))?;
                table
                    .insert(key.as_str(), value.as_slice())
                    .map_err(|e| SaciError::store(format!("redb put prior {key}: {e}")))?;
            }
            {
                let mut htable = txn
                    .open_table(PRIOR_HISTORY_TABLE)
                    .map_err(|e| SaciError::store(format!("redb open prior history: {e}")))?;
                htable
                    .insert(hist_key.as_str(), value.as_slice())
                    .map_err(|e| SaciError::store(format!("redb put prior history: {e}")))?;
                prune_prefix(&mut htable, &hist_prefix)?;
            }
            txn.commit()
                .map_err(|e| SaciError::store(format!("redb commit: {e}")))
        })
        .await
    }

    /// Remove a processor's persisted state blob (a cleared state must not
    /// resurrect on restart).
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Store`] on a redb failure.
    pub async fn delete_prior(&self, workflow_id: &str, node_id: &str) -> SaciResult<()> {
        let db = Arc::clone(&self.db);
        let key = join(workflow_id, node_id);
        blocking("delete prior", move || {
            let db = lock(&db)?;
            let txn = db
                .begin_write()
                .map_err(|e| SaciError::store(format!("redb begin_write: {e}")))?;
            {
                let mut table = txn
                    .open_table(PRIORS_TABLE)
                    .map_err(|e| SaciError::store(format!("redb open prior table: {e}")))?;
                table
                    .remove(key.as_str())
                    .map_err(|e| SaciError::store(format!("redb delete prior {key}: {e}")))?;
            }
            txn.commit()
                .map_err(|e| SaciError::store(format!("redb commit: {e}")))
        })
        .await
    }

    /// Load a source's persisted cursor, if any.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Store`] on a redb or decode failure.
    pub async fn load_source_cursor(
        &self,
        workflow_id: &str,
        source_id: &str,
    ) -> SaciResult<Option<SourceCursorMeta>> {
        let Some(bytes) = self
            .get(CURSORS_TABLE, join(workflow_id, source_id), "cursor")
            .await?
        else {
            return Ok(None);
        };
        let meta = postcard::from_bytes(&bytes)
            .map_err(|e| SaciError::store(format!("redb decode cursor: {e}")))?;
        Ok(Some(meta))
    }

    /// Persist a source's cursor and record a timestamped history entry.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Store`] on a redb or encode failure.
    pub async fn save_source_cursor(
        &self,
        workflow_id: &str,
        source_id: &str,
        meta: SourceCursorMeta,
    ) -> SaciResult<()> {
        let bytes = postcard::to_allocvec(&meta)
            .map_err(|e| SaciError::store(format!("redb encode cursor: {e}")))?;
        let db = Arc::clone(&self.db);
        let key = join(workflow_id, source_id);
        let hist_prefix = format!("{key}/");
        let hist_key = format!("{hist_prefix}{:020}", meta.last_batch_at_ms);
        blocking("cursor", move || {
            let db = lock(&db)?;
            let txn = db
                .begin_write()
                .map_err(|e| SaciError::store(format!("redb begin_write: {e}")))?;
            {
                let mut table = txn
                    .open_table(CURSORS_TABLE)
                    .map_err(|e| SaciError::store(format!("redb open cursor table: {e}")))?;
                table
                    .insert(key.as_str(), bytes.as_slice())
                    .map_err(|e| SaciError::store(format!("redb put cursor {key}: {e}")))?;
            }
            {
                let mut htable = txn
                    .open_table(CURSOR_HISTORY_TABLE)
                    .map_err(|e| SaciError::store(format!("redb open cursor history: {e}")))?;
                htable
                    .insert(hist_key.as_str(), bytes.as_slice())
                    .map_err(|e| SaciError::store(format!("redb put cursor history: {e}")))?;
                prune_prefix(&mut htable, &hist_prefix)?;
            }
            txn.commit()
                .map_err(|e| SaciError::store(format!("redb commit: {e}")))
        })
        .await
    }

    /// Remove a source's persisted cursor.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Store`] on a redb failure.
    pub async fn delete_source_cursor(&self, workflow_id: &str, source_id: &str) -> SaciResult<()> {
        let db = Arc::clone(&self.db);
        let key = join(workflow_id, source_id);
        blocking("delete cursor", move || {
            let db = lock(&db)?;
            let txn = db
                .begin_write()
                .map_err(|e| SaciError::store(format!("redb begin_write: {e}")))?;
            {
                let mut table = txn
                    .open_table(CURSORS_TABLE)
                    .map_err(|e| SaciError::store(format!("redb open cursor table: {e}")))?;
                table
                    .remove(key.as_str())
                    .map_err(|e| SaciError::store(format!("redb delete cursor {key}: {e}")))?;
            }
            txn.commit()
                .map_err(|e| SaciError::store(format!("redb commit: {e}")))
        })
        .await
    }

    /// Inspect current source cursors, persisted processor state IDs, and
    /// available historical checkpoint timestamps for `workflow_id`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Store`] on a redb or decode failure.
    pub async fn inspect_workflow(
        &self,
        workflow_id: &str,
        source_ids: &[String],
        processor_ids: &[String],
    ) -> SaciResult<WorkflowCursorsReport> {
        let db = Arc::clone(&self.db);
        let wf = workflow_id.to_string();
        let sources = source_ids.to_vec();
        let processors = processor_ids.to_vec();
        blocking("inspect workflow state", move || {
            let db = lock(&db)?;
            let txn = db
                .begin_read()
                .map_err(|e| SaciError::store(format!("redb begin_read: {e}")))?;

            let mut cursors = Vec::new();
            match txn.open_table(CURSORS_TABLE) {
                Ok(table) => {
                    for sid in &sources {
                        let key = join(&wf, sid);
                        if let Some(val) = table
                            .get(key.as_str())
                            .map_err(|e| SaciError::store(format!("redb get cursor: {e}")))?
                        {
                            let meta: SourceCursorMeta = postcard::from_bytes(val.value())
                                .map_err(|e| {
                                    SaciError::store(format!("redb decode cursor: {e}"))
                                })?;
                            cursors.push(CursorSnapshot {
                                source: sid.clone(),
                                items_processed: meta.items_processed,
                                last_batch_at_ms: meta.last_batch_at_ms,
                            });
                        }
                    }
                }
                Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(e) => return Err(SaciError::store(format!("redb open cursor table: {e}"))),
            }

            let mut processors_with_state = Vec::new();
            match txn.open_table(PRIORS_TABLE) {
                Ok(table) => {
                    for pid in &processors {
                        let key = join(&wf, pid);
                        if table
                            .get(key.as_str())
                            .map_err(|e| SaciError::store(format!("redb get prior: {e}")))?
                            .is_some()
                        {
                            processors_with_state.push(pid.clone());
                        }
                    }
                }
                Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(e) => return Err(SaciError::store(format!("redb open prior table: {e}"))),
            }

            let mut checkpoints = std::collections::BTreeSet::new();
            for table_def in [CURSOR_HISTORY_TABLE, PRIOR_HISTORY_TABLE] {
                match txn.open_table(table_def) {
                    Ok(table) => {
                        let wf_prefix = format!("{wf}/");
                        for entry in table
                            .range(wf_prefix.as_str()..)
                            .map_err(|e| SaciError::store(format!("redb range history: {e}")))?
                        {
                            let (k, _) = entry
                                .map_err(|e| SaciError::store(format!("redb iter history: {e}")))?;
                            let ks = k.value();
                            if !ks.starts_with(&wf_prefix) {
                                break;
                            }
                            if let Some((_, ts_str)) = ks.rsplit_once('/')
                                && let Ok(ts) = ts_str.parse::<u64>()
                            {
                                checkpoints.insert(ts);
                            }
                        }
                    }
                    Err(redb::TableError::TableDoesNotExist(_)) => {}
                    Err(e) => {
                        return Err(SaciError::store(format!("redb open history table: {e}")));
                    }
                }
            }

            Ok(WorkflowCursorsReport {
                workflow: wf,
                cursors,
                processors_with_state,
                checkpoints_unix_ms: checkpoints.into_iter().collect(),
            })
        })
        .await
    }

    /// Reset or time-travel source cursors and processor priors for `workflow_id`.
    ///
    /// - If `items_processed` is `Some(n)`, sets each target source's cursor to `n`.
    /// - Else if `to_unix_ms` is `Some(ts)`, restores the latest historical snapshot
    ///   at or before `ts` (or resets to `0` at `ts` when no snapshot precedes `ts`).
    /// - Else clears the cursor(s) to origin.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Store`] on a redb or codec failure.
    #[allow(clippy::too_many_arguments)]
    pub async fn reset_workflow(
        &self,
        workflow_id: &str,
        target_sources: &[String],
        processor_ids: &[String],
        to_unix_ms: Option<u64>,
        items_processed: Option<u64>,
        reset_priors: bool,
    ) -> SaciResult<CursorResetReport> {
        let db = Arc::clone(&self.db);
        let wf = workflow_id.to_string();
        let targets = target_sources.to_vec();
        let processors = processor_ids.to_vec();

        blocking("reset workflow cursors", move || {
            let db = lock(&db)?;
            let txn = db
                .begin_write()
                .map_err(|e| SaciError::store(format!("redb begin_write: {e}")))?;

            let mut restored_checkpoint: Option<u64> = None;
            let mut result_cursors = Vec::new();
            let mut priors_reset = Vec::new();

            {
                let mut ctable = txn
                    .open_table(CURSORS_TABLE)
                    .map_err(|e| SaciError::store(format!("redb open cursor table: {e}")))?;
                let chist = txn
                    .open_table(CURSOR_HISTORY_TABLE)
                    .map_err(|e| SaciError::store(format!("redb open cursor history: {e}")))?;

                for sid in &targets {
                    let key = join(&wf, sid);
                    if let Some(items) = items_processed {
                        let meta = SourceCursorMeta {
                            items_processed: items,
                            last_batch_at_ms: to_unix_ms.unwrap_or_else(now_unix_ms),
                        };
                        let bytes = postcard::to_allocvec(&meta)
                            .map_err(|e| SaciError::store(format!("redb encode cursor: {e}")))?;
                        ctable
                            .insert(key.as_str(), bytes.as_slice())
                            .map_err(|e| SaciError::store(format!("redb put cursor: {e}")))?;
                        result_cursors.push(CursorSnapshot {
                            source: sid.clone(),
                            items_processed: meta.items_processed,
                            last_batch_at_ms: meta.last_batch_at_ms,
                        });
                    } else if let Some(target_ts) = to_unix_ms {
                        let prefix = format!("{key}/");
                        let best = find_latest_before(&chist, &prefix, target_ts)?;
                        let meta = match best {
                            Some((ts, bytes)) => {
                                restored_checkpoint =
                                    Some(restored_checkpoint.map_or(ts, |prev| prev.max(ts)));
                                postcard::from_bytes::<SourceCursorMeta>(&bytes).map_err(|e| {
                                    SaciError::store(format!("redb decode cursor: {e}"))
                                })?
                            }
                            None => SourceCursorMeta {
                                items_processed: 0,
                                last_batch_at_ms: target_ts,
                            },
                        };
                        let bytes = postcard::to_allocvec(&meta)
                            .map_err(|e| SaciError::store(format!("redb encode cursor: {e}")))?;
                        ctable
                            .insert(key.as_str(), bytes.as_slice())
                            .map_err(|e| SaciError::store(format!("redb put cursor: {e}")))?;
                        result_cursors.push(CursorSnapshot {
                            source: sid.clone(),
                            items_processed: meta.items_processed,
                            last_batch_at_ms: meta.last_batch_at_ms,
                        });
                    } else {
                        ctable
                            .remove(key.as_str())
                            .map_err(|e| SaciError::store(format!("redb delete cursor: {e}")))?;
                    }
                }
            }

            if reset_priors {
                let mut ptable = txn
                    .open_table(PRIORS_TABLE)
                    .map_err(|e| SaciError::store(format!("redb open prior table: {e}")))?;
                let phist = txn
                    .open_table(PRIOR_HISTORY_TABLE)
                    .map_err(|e| SaciError::store(format!("redb open prior history: {e}")))?;

                for pid in &processors {
                    let key = join(&wf, pid);
                    if let Some(target_ts) = to_unix_ms
                        && items_processed.is_none()
                    {
                        let prefix = format!("{key}/");
                        if let Some((ts, blob)) = find_latest_before(&phist, &prefix, target_ts)? {
                            restored_checkpoint =
                                Some(restored_checkpoint.map_or(ts, |prev| prev.max(ts)));
                            ptable
                                .insert(key.as_str(), blob.as_slice())
                                .map_err(|e| SaciError::store(format!("redb put prior: {e}")))?;
                            priors_reset.push(pid.clone());
                            continue;
                        }
                    }
                    if ptable
                        .remove(key.as_str())
                        .map_err(|e| SaciError::store(format!("redb delete prior: {e}")))?
                        .is_some()
                    {
                        priors_reset.push(pid.clone());
                    }
                }
            }

            txn.commit()
                .map_err(|e| SaciError::store(format!("redb commit: {e}")))?;

            Ok(CursorResetReport {
                workflow: wf,
                cursors: result_cursors,
                priors_reset,
                restored_checkpoint_unix_ms: restored_checkpoint,
            })
        })
        .await
    }

    async fn put(
        &self,
        table: TableDefinition<'static, &'static str, &'static [u8]>,
        key: String,
        value: Vec<u8>,
        what: &'static str,
    ) -> SaciResult<()> {
        let db = Arc::clone(&self.db);
        blocking(what, move || {
            let db = lock(&db)?;
            let txn = db
                .begin_write()
                .map_err(|e| SaciError::store(format!("redb begin_write: {e}")))?;
            {
                let mut table = txn
                    .open_table(table)
                    .map_err(|e| SaciError::store(format!("redb open {what} table: {e}")))?;
                table
                    .insert(key.as_str(), value.as_slice())
                    .map_err(|e| SaciError::store(format!("redb put {what} {key}: {e}")))?;
            }
            txn.commit()
                .map_err(|e| SaciError::store(format!("redb commit: {e}")))
        })
        .await
    }

    async fn get(
        &self,
        table: TableDefinition<'static, &'static str, &'static [u8]>,
        key: String,
        what: &'static str,
    ) -> SaciResult<Option<Vec<u8>>> {
        let db = Arc::clone(&self.db);
        blocking(what, move || {
            let db = lock(&db)?;
            let txn = db
                .begin_read()
                .map_err(|e| SaciError::store(format!("redb begin_read: {e}")))?;
            let table = match txn.open_table(table) {
                Ok(t) => t,
                // A table is created by the first write, so an absent one is
                // simply "nothing persisted yet".
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(e) => {
                    return Err(SaciError::store(format!("redb open {what} table: {e}")));
                }
            };
            let value = table
                .get(key.as_str())
                .map_err(|e| SaciError::store(format!("redb get {what} {key}: {e}")))?;
            Ok(value.map(|v| v.value().to_vec()))
        })
        .await
    }
}

/// Compose the two-segment key both the priors and the cursors table use.
fn join(workflow_id: &str, node_id: &str) -> String {
    format!("{workflow_id}/{node_id}")
}

/// Keep at most `MAX_HISTORY_PER_NODE` entries under `prefix`.
fn prune_prefix(
    table: &mut redb::Table<'_, &'static str, &'static [u8]>,
    prefix: &str,
) -> SaciResult<()> {
    let mut keys = Vec::new();
    for entry in table
        .range(prefix..)
        .map_err(|e| SaciError::store(format!("redb range history: {e}")))?
    {
        let (k, _) = entry.map_err(|e| SaciError::store(format!("redb iter history: {e}")))?;
        let ks = k.value();
        if !ks.starts_with(prefix) {
            break;
        }
        keys.push(ks.to_string());
    }
    if keys.len() > MAX_HISTORY_PER_NODE {
        let excess = keys.len() - MAX_HISTORY_PER_NODE;
        for k in keys.into_iter().take(excess) {
            table
                .remove(k.as_str())
                .map_err(|e| SaciError::store(format!("redb prune history: {e}")))?;
        }
    }
    Ok(())
}

/// Find the newest `(timestamp_ms, value)` under `prefix` with `ts <= target_ts`.
fn find_latest_before(
    table: &redb::Table<'_, &'static str, &'static [u8]>,
    prefix: &str,
    target_ts: u64,
) -> SaciResult<Option<(u64, Vec<u8>)>> {
    let mut best: Option<(u64, Vec<u8>)> = None;
    for entry in table
        .range(prefix..)
        .map_err(|e| SaciError::store(format!("redb range history: {e}")))?
    {
        let (k, v) = entry.map_err(|e| SaciError::store(format!("redb iter history: {e}")))?;
        let ks = k.value();
        if !ks.starts_with(prefix) {
            break;
        }
        let Some(ts_str) = ks.strip_prefix(prefix) else {
            continue;
        };
        let Ok(ts) = ts_str.parse::<u64>() else {
            continue;
        };
        if ts <= target_ts {
            best = Some((ts, v.value().to_vec()));
        } else {
            break;
        }
    }
    Ok(best)
}

fn lock(db: &Mutex<Database>) -> SaciResult<std::sync::MutexGuard<'_, Database>> {
    db.lock()
        .map_err(|_| SaciError::store("state DB mutex poisoned"))
}

/// Run one redb transaction off the runtime: a commit fsyncs.
async fn blocking<T, F>(what: &'static str, f: F) -> SaciResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> SaciResult<T> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| SaciError::store(format!("redb {what} task panicked: {e}")))?
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client() -> (RedbStateClient, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let client = RedbStateClient::open(&dir.path().join("state.redb")).expect("open");
        (client, dir)
    }

    /// A read against a file no write has touched yet reports "nothing
    /// persisted", not an error: the table is created by the first write.
    #[tokio::test]
    async fn reads_before_any_write_are_empty() {
        let (client, _dir) = client();
        assert_eq!(client.load_prior("w", "n").await.unwrap(), None);
        assert_eq!(client.load_source_cursor("w", "s").await.unwrap(), None);
    }

    #[tokio::test]
    async fn prior_roundtrips_and_delete_clears_it() {
        let (client, _dir) = client();
        client.save_prior("w", "n", b"state").await.unwrap();
        assert_eq!(
            client.load_prior("w", "n").await.unwrap(),
            Some(b"state".to_vec())
        );
        client.delete_prior("w", "n").await.unwrap();
        assert_eq!(client.load_prior("w", "n").await.unwrap(), None);
    }

    /// Keys are per workflow **and** per node, so two nodes of one workflow do
    /// not read each other's state.
    #[tokio::test]
    async fn priors_are_keyed_per_node() {
        let (client, _dir) = client();
        client.save_prior("w", "a", b"a-state").await.unwrap();
        client.save_prior("w", "b", b"b-state").await.unwrap();
        assert_eq!(
            client.load_prior("w", "a").await.unwrap(),
            Some(b"a-state".to_vec())
        );
        assert_eq!(
            client.load_prior("w", "b").await.unwrap(),
            Some(b"b-state".to_vec())
        );
    }

    #[tokio::test]
    async fn cursor_roundtrips() {
        let (client, _dir) = client();
        let meta = SourceCursorMeta {
            items_processed: 7,
            last_batch_at_ms: 1_700_000_000_000,
        };
        client.save_source_cursor("w", "s", meta).await.unwrap();
        assert_eq!(
            client.load_source_cursor("w", "s").await.unwrap(),
            Some(meta)
        );
    }

    /// Reopening the same file must see what the previous handle committed:
    /// that is the whole point of the store.
    #[tokio::test]
    async fn values_survive_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("state.redb");
        {
            let client = RedbStateClient::open(&path).expect("open");
            client
                .put_config("saci.kdl", b"mode \"standalone\"")
                .await
                .unwrap();
            client.save_prior("w", "n", b"blob").await.unwrap();
        }
        let client = RedbStateClient::open(&path).expect("reopen");
        assert_eq!(
            client.load_prior("w", "n").await.unwrap(),
            Some(b"blob".to_vec())
        );
    }
}
