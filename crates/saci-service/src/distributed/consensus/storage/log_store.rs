//! Raft log storage in a dedicated redb file.
//!
//! Holds [`ArrowRedbLogStore`] and its [`RaftLogReader`] / [`RaftLogStorage`]
//! implementations. Raft metadata (vote, purged log id) and log entries only, no
//! Arrow data. Every trait-method redb transaction runs inside
//! [`tokio::task::spawn_blocking`]; the parent module doc explains why.

use std::fmt::Debug;
use std::io;
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::Arc;

use openraft::RaftLogReader;
use openraft::storage::{IOFlushed, LogState, RaftLogStorage};
use openraft::type_config::alias::{EntryOf, LogIdOf, VoteOf};
use redb::{Database, ReadableDatabase, ReadableTable};

use super::{ENTRIES_TABLE, KEY_PURGED_LOG_ID, KEY_VOTE, META_TABLE, dec, enc, join_to_io, to_io};
use crate::distributed::consensus::types::SaciTypeConfig;
use crate::error::{SaciError, SaciResult};

/// Materialise an owned `(Bound<u64>, Bound<u64>)` pair from an arbitrary
/// `RangeBounds<u64>`, so it can be moved into a `spawn_blocking` closure
/// without forcing the caller's range type to be `'static`.
fn owned_bounds<RB: RangeBounds<u64>>(range: &RB) -> (Bound<u64>, Bound<u64>) {
    let start = match range.start_bound() {
        Bound::Included(v) => Bound::Included(*v),
        Bound::Excluded(v) => Bound::Excluded(*v),
        Bound::Unbounded => Bound::Unbounded,
    };
    let end = match range.end_bound() {
        Bound::Included(v) => Bound::Included(*v),
        Bound::Excluded(v) => Bound::Excluded(*v),
        Bound::Unbounded => Bound::Unbounded,
    };
    (start, end)
}

/// Persistent Raft log storage in a dedicated redb file.
///
/// Contains only Raft metadata and log entries; no Arrow data.
///
/// Cloning is cheap (an `Arc` bump). All trait-method redb I/O runs inside
/// [`tokio::task::spawn_blocking`]; see the module-level doc comment for the reason.
#[derive(Clone)]
pub struct ArrowRedbLogStore {
    /// `redb::Database` coordinates concurrent readers and a single writer internally,
    /// and both `begin_read` and `begin_write` take `&self`. An external mutex would
    /// only serialise readers unnecessarily.
    db: Arc<Database>,
}

impl ArrowRedbLogStore {
    /// Open (or create) the log store at `path`.
    ///
    /// Initial table creation runs synchronously on the caller thread
    /// because this is a one-shot open path, not a hot trait method. The
    /// `fsync` cost at open time is acceptable.
    pub fn open(path: impl AsRef<Path>) -> SaciResult<Self> {
        let db = Database::create(path.as_ref())
            .map_err(|e| SaciError::store(format!("open arrow log redb: {e}")))?;
        {
            let txn = db
                .begin_write()
                .map_err(|e| SaciError::store(e.to_string()))?;
            txn.open_table(META_TABLE)
                .map_err(|e| SaciError::store(e.to_string()))?;
            txn.open_table(ENTRIES_TABLE)
                .map_err(|e| SaciError::store(e.to_string()))?;
            txn.commit().map_err(|e| SaciError::store(e.to_string()))?;
        }
        Ok(Self { db: Arc::new(db) })
    }
}

impl RaftLogReader<SaciTypeConfig> for ArrowRedbLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf<SaciTypeConfig>>, io::Error> {
        let db = Arc::clone(&self.db);
        let bounds = owned_bounds(&range);
        tokio::task::spawn_blocking(move || -> io::Result<_> {
            let txn = db.begin_read().map_err(to_io)?;
            let table = txn.open_table(ENTRIES_TABLE).map_err(to_io)?;
            let mut out = Vec::new();
            for item in table.range::<u64>(bounds).map_err(to_io)? {
                let (_k, v) = item.map_err(to_io)?;
                out.push(dec(v.value())?);
            }
            Ok(out)
        })
        .await
        .map_err(join_to_io)?
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<SaciTypeConfig>>, io::Error> {
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || -> io::Result<_> {
            let txn = db.begin_read().map_err(to_io)?;
            let table = txn.open_table(META_TABLE).map_err(to_io)?;
            match table.get(KEY_VOTE).map_err(to_io)? {
                Some(v) => dec::<VoteOf<SaciTypeConfig>>(v.value()).map(Some),
                None => Ok(None),
            }
        })
        .await
        .map_err(join_to_io)?
    }
}

impl RaftLogStorage<SaciTypeConfig> for ArrowRedbLogStore {
    type LogReader = ArrowRedbLogStore;

    async fn get_log_state(&mut self) -> Result<LogState<SaciTypeConfig>, io::Error> {
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || -> io::Result<LogState<SaciTypeConfig>> {
            let txn = db.begin_read().map_err(to_io)?;
            // Read purged log id.
            let purged: Option<LogIdOf<SaciTypeConfig>> = {
                let meta = txn.open_table(META_TABLE).map_err(to_io)?;
                match meta.get(KEY_PURGED_LOG_ID).map_err(to_io)? {
                    Some(v) => Some(dec(v.value())?),
                    None => None,
                }
            };
            // Read last entry log id.
            let last: Option<LogIdOf<SaciTypeConfig>> = {
                let entries = txn.open_table(ENTRIES_TABLE).map_err(to_io)?;
                match entries.last().map_err(to_io)? {
                    Some((_k, v)) => {
                        let entry: EntryOf<SaciTypeConfig> = dec(v.value())?;
                        Some(entry.log_id)
                    }
                    None => None,
                }
            };
            Ok(LogState {
                last_purged_log_id: purged,
                last_log_id: last.or(purged),
            })
        })
        .await
        .map_err(join_to_io)?
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &VoteOf<SaciTypeConfig>) -> Result<(), io::Error> {
        // Encode before moving into the blocking task: keeps the closure cheap and
        // avoids pushing serde work onto the blocking pool.
        let bytes = enc(vote)?;
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || -> io::Result<()> {
            let txn = db.begin_write().map_err(to_io)?;
            {
                let mut table = txn.open_table(META_TABLE).map_err(to_io)?;
                table.insert(KEY_VOTE, bytes.as_slice()).map_err(to_io)?;
            }
            txn.commit().map_err(to_io)
        })
        .await
        .map_err(join_to_io)?
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<SaciTypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<SaciTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        // Encode entries on the caller thread. The encode cost is paid once per entry,
        // and keeping it off the blocking pool leaves the blocking task as pure disk
        // I/O: write and fsync. An encode failure fails fast without touching redb.
        let encoded: Vec<(u64, Vec<u8>)> = entries
            .into_iter()
            .map(|e| {
                let idx = e.log_id.index;
                enc(&e).map(|bytes| (idx, bytes))
            })
            .collect::<io::Result<_>>()?;

        let db = Arc::clone(&self.db);
        let res = tokio::task::spawn_blocking(move || -> io::Result<()> {
            let txn = db.begin_write().map_err(to_io)?;
            {
                let mut table = txn.open_table(ENTRIES_TABLE).map_err(to_io)?;
                for (idx, bytes) in &encoded {
                    table.insert(*idx, bytes.as_slice()).map_err(to_io)?;
                }
            }
            txn.commit().map_err(to_io)
        })
        .await
        .map_err(join_to_io)?;

        // Fire the flush callback only on success so openraft does not
        // advance its durable-commit watermark past an unpersisted batch.
        res?;
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(
        &mut self,
        last: Option<LogIdOf<SaciTypeConfig>>,
    ) -> Result<(), io::Error> {
        let from = last.as_ref().map_or(0, |l| l.index + 1);
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || -> io::Result<()> {
            let txn = db.begin_write().map_err(to_io)?;
            {
                let mut table = txn.open_table(ENTRIES_TABLE).map_err(to_io)?;
                let to_remove: Vec<u64> = table
                    .range(from..)
                    .map_err(to_io)?
                    .map(|r| r.map(|(k, _)| k.value()).map_err(to_io))
                    .collect::<io::Result<_>>()?;
                for idx in to_remove {
                    table.remove(idx).map_err(to_io)?;
                }
            }
            txn.commit().map_err(to_io)
        })
        .await
        .map_err(join_to_io)?
    }

    async fn purge(&mut self, log_id: LogIdOf<SaciTypeConfig>) -> Result<(), io::Error> {
        let up_to = log_id.index;
        // Encode the purge marker on this thread so the blocking closure does no serde
        // work.
        let marker = enc(&log_id)?;
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || -> io::Result<()> {
            let txn = db.begin_write().map_err(to_io)?;
            {
                let mut meta = txn.open_table(META_TABLE).map_err(to_io)?;
                let mut entries = txn.open_table(ENTRIES_TABLE).map_err(to_io)?;
                meta.insert(KEY_PURGED_LOG_ID, marker.as_slice())
                    .map_err(to_io)?;
                let to_remove: Vec<u64> = entries
                    .range(..=up_to)
                    .map_err(to_io)?
                    .map(|r| r.map(|(k, _)| k.value()).map_err(to_io))
                    .collect::<io::Result<_>>()?;
                for idx in to_remove {
                    entries.remove(idx).map_err(to_io)?;
                }
            }
            txn.commit().map_err(to_io)
        })
        .await
        .map_err(join_to_io)?
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{blank_entry, log_id, make_store};
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_empty_state() {
        let dir = TempDir::new().unwrap();
        let mut store = make_store(&dir);
        let state = store.get_log_state().await.unwrap();
        assert!(state.last_purged_log_id.is_none());
        assert!(state.last_log_id.is_none());
    }

    #[tokio::test]
    async fn test_vote_round_trip() {
        let dir = TempDir::new().unwrap();
        let mut store = make_store(&dir);
        let vote = openraft::Vote::new(1, 2);
        store.save_vote(&vote).await.unwrap();
        assert_eq!(store.read_vote().await.unwrap(), Some(vote));
    }

    #[tokio::test]
    async fn test_append_and_read() {
        let dir = TempDir::new().unwrap();
        let mut store = make_store(&dir);
        store
            .append(
                vec![blank_entry(1), blank_entry(2), blank_entry(3)],
                IOFlushed::noop(),
            )
            .await
            .unwrap();
        let read = store.try_get_log_entries(1..4).await.unwrap();
        assert_eq!(read.len(), 3);
        assert_eq!(read[0].log_id.index, 1);
    }

    #[tokio::test]
    async fn test_truncate() {
        let dir = TempDir::new().unwrap();
        let mut store = make_store(&dir);
        store
            .append(
                vec![blank_entry(1), blank_entry(2), blank_entry(3)],
                IOFlushed::noop(),
            )
            .await
            .unwrap();
        store.truncate_after(Some(log_id(1, 1))).await.unwrap();
        let remaining = store.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(remaining.len(), 1);
    }

    #[tokio::test]
    async fn test_purge() {
        let dir = TempDir::new().unwrap();
        let mut store = make_store(&dir);
        store
            .append(
                vec![blank_entry(1), blank_entry(2), blank_entry(3)],
                IOFlushed::noop(),
            )
            .await
            .unwrap();
        store.purge(log_id(1, 2)).await.unwrap();
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.unwrap().index, 2);
    }

    /// `truncate_after` removes entries above `last`.
    #[tokio::test]
    async fn truncate_after_removes_tail() {
        let dir = TempDir::new().unwrap();
        let mut store = make_store(&dir);
        store
            .append(
                vec![
                    blank_entry(1),
                    blank_entry(2),
                    blank_entry(3),
                    blank_entry(4),
                ],
                IOFlushed::noop(),
            )
            .await
            .unwrap();

        // Truncate after index 2 → 3 and 4 should be removed.
        store.truncate_after(Some(log_id(1, 2))).await.unwrap();
        let remaining = store.try_get_log_entries(1..10).await.unwrap();
        assert_eq!(remaining.len(), 2, "only indices 1 and 2 should remain");
        assert_eq!(remaining[0].log_id.index, 1);
        assert_eq!(remaining[1].log_id.index, 2);
    }
}
