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
//! - `saci_config`: config name to the raw (pre-substitution) KDL bytes.
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

use redb::{Database, ReadableDatabase, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::SaciResult;
use crate::error::SaciError;

/// Config name to the raw KDL bytes it was loaded from.
const CONFIG_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("saci_config");
/// `"{workflow_id}/{node_id}"` to a runtime's opaque state blob.
const PRIORS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("saci_priors");
/// `"{workflow_id}/{source_id}"` to a postcard-encoded [`SourceCursorMeta`].
const CURSORS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("saci_cursors");

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

    /// Persist a processor's state blob.
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
        self.put(
            PRIORS_TABLE,
            join(workflow_id, node_id),
            blob.to_vec(),
            "prior",
        )
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

    /// Persist a source's cursor.
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
        self.put(CURSORS_TABLE, join(workflow_id, source_id), bytes, "cursor")
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
