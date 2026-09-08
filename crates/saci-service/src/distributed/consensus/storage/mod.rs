//! Persistent Raft log storage and Arrow-IPC state machine backed by separate redb files.
//!
//! ## Separate redb files
//!
//! Arrow blob storage in the state machine competes with log storage for write I/O, so
//! the two are kept strictly separate:
//!
//! - **Log store** (`ArrowRedbLogStore`, in `log_store`): Raft metadata (vote, purged
//!   log id) and log entries only. No Arrow data lives here.
//!
//! - **State machine** (`ArrowRedbStateMachine`, in `state_machine_store`): drives the
//!   Arrow-IPC application state through `sm_apply`. Arrow IPC bytes are written in the
//!   same transaction as the log apply, so the fsync is fate-shared.
//!
//! Both halves share the redb table definitions, the `postcard` encode/decode helpers,
//! and the `validate_store_consistency` startup guard, all defined in this file.
//!
//! ## Blocking-I/O discipline
//!
//! All redb read/write transactions in `ArrowRedbLogStore` are wrapped in
//! [`tokio::task::spawn_blocking`]. redb's commit path issues `fsync`, which must not
//! run on a tokio worker thread: blocking a worker for fsync latency stalls every
//! other task on that runtime. The log store holds an `Arc<Database>` and no
//! [`std::sync::Mutex`], because redb's `Database::begin_write` / `begin_read` both
//! take `&self` and coordinate internally, so external serialization would only add
//! lock contention.
//!
//! ## Log entry encoding
//!
//! Log entries are encoded with `postcard`. Log files written by `pre-1.0.0-alpha.1`
//! builds used `serde_json` and are not decodable; wipe the Raft log and state-machine
//! redb files before starting after an upgrade from those builds.
//!
//! Postcard is canonical by construction: the same input always produces the same
//! bytes, which matters if log entries are ever content-hashed. It also avoids UTF-8
//! encoding cost on append and apply, both of which run under spawn_blocking and
//! contend for disk, and it has no JSON map-ordering ambiguity if a nested type grows
//! a map field.

#[cfg(feature = "distributed-raft")]
#[path = "."]
pub(crate) mod raft_impl {
    mod log_store;
    mod state_machine_store;

    use std::io;

    use openraft::type_config::alias::LogIdOf;
    use redb::TableDefinition;

    use crate::distributed::consensus::types::SaciTypeConfig;
    use crate::error::{SaciError, SaciResult};

    pub use log_store::ArrowRedbLogStore;
    pub use state_machine_store::ArrowRedbStateMachine;

    /// Log-store tables (live in the log redb file).
    const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("arrow_raft_meta");
    const ENTRIES_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("arrow_raft_entries");

    const KEY_VOTE: &str = "vote";
    const KEY_PURGED_LOG_ID: &str = "purged_log_id";

    /// State-machine metadata table (lives in the *app* redb file).
    ///
    /// Stores `last_applied` and `last_membership` so they survive restarts.
    /// Using a separate table keeps SM metadata writes fate-shared with the
    /// application data in the same redb file, so one fsync covers both.
    const SM_META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("arrow_sm_meta");

    fn enc<T: serde::Serialize>(v: &T) -> io::Result<Vec<u8>> {
        postcard::to_allocvec(v).map_err(|e| io::Error::other(format!("postcard encode: {e}")))
    }

    fn dec<T: for<'de> serde::Deserialize<'de>>(b: &[u8]) -> io::Result<T> {
        postcard::from_bytes(b).map_err(|e| io::Error::other(format!("postcard decode: {e}")))
    }

    fn to_io(e: impl std::error::Error) -> io::Error {
        io::Error::other(e.to_string())
    }

    /// Convert a `tokio::task::JoinError` to an `io::Error`. A JoinError means the
    /// blocking task panicked or was cancelled; both are hard I/O failures, so openraft
    /// can surface the storage error upward.
    fn join_to_io(e: tokio::task::JoinError) -> io::Error {
        io::Error::other(format!("blocking redb task failed: {e}"))
    }

    /// Validate that `last_applied` is not behind `last_purged_log_id`.
    ///
    /// Call this after opening both `ArrowRedbLogStore` and
    /// `ArrowRedbStateMachine` from the same node directory. A mismatch
    /// indicates the files were restored from mismatched backups or the log
    /// store was wiped while the state-machine file was retained.
    ///
    /// Returns `Ok(())` when consistent. Returns `Err` with a diagnostic
    /// message when the invariant is violated.
    ///
    /// # Safety
    ///
    /// This is a diagnostic check only and modifies no state.
    /// Pass `None` for either argument if the corresponding watermark has
    /// not yet been written (e.g. a freshly-initialized node).
    pub fn validate_store_consistency(
        last_purged: Option<LogIdOf<SaciTypeConfig>>,
        last_applied: Option<LogIdOf<SaciTypeConfig>>,
    ) -> SaciResult<()> {
        match (last_purged, last_applied) {
            (Some(purged), None) => Err(SaciError::store(format!(
                "store consistency violation: log store has purged up to index {} \
                 but state machine last_applied is None — state machine is behind. \
                 Do not mix log and state-machine redb files from different backups.",
                purged.index
            ))),
            (Some(purged), Some(applied)) if applied.index < purged.index => {
                Err(SaciError::store(format!(
                    "store consistency violation: log store purged up to index {} \
                     but state machine last_applied is index {} — state machine is behind. \
                     Do not mix log and state-machine redb files from different backups.",
                    purged.index, applied.index
                )))
            }
            _ => Ok(()),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::distributed::consensus::state_machine::KEY_SM_LAST_APPLIED;
        use openraft::entry::RaftEntry;
        use openraft::type_config::alias::EntryOf;
        use redb::{Database, ReadableDatabase};
        use tempfile::TempDir;

        pub(super) fn make_store(dir: &TempDir) -> ArrowRedbLogStore {
            ArrowRedbLogStore::open(dir.path().join("arrow_raft.db")).unwrap()
        }

        pub(super) fn log_id(
            term: u64,
            index: u64,
        ) -> openraft::type_config::alias::LogIdOf<SaciTypeConfig> {
            use openraft::vote::RaftLeaderId;
            openraft::LogId::new(
                openraft::impls::leader_id_adv::LeaderId::new(term, 1u64),
                index,
            )
        }

        pub(super) fn blank_entry(index: u64) -> EntryOf<SaciTypeConfig> {
            openraft::Entry::new_blank(log_id(1, index))
        }

        /// Verify postcard enc/dec round-trip for Option<LogId>.
        #[test]
        fn test_postcard_log_id_round_trip() {
            let lid = log_id(2, 10);
            let opt: Option<LogIdOf<SaciTypeConfig>> = Some(lid);
            let bytes = enc(&opt).unwrap();
            let decoded: Option<LogIdOf<SaciTypeConfig>> = dec(&bytes).unwrap();
            assert_eq!(decoded.map(|l| l.index), Some(10));
        }

        /// An empty begin_write + open_table + commit must not destroy data already in
        /// the table. `ArrowRedbStateMachine::open()` relies on this to be restart-safe.
        #[test]
        fn test_redb_open_table_preserves_existing_data() {
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("preserve_test.redb");

            {
                let db = Database::create(&path).unwrap();
                let txn = db.begin_write().unwrap();
                {
                    let mut t = txn.open_table(SM_META_TABLE).unwrap();
                    t.insert(KEY_SM_LAST_APPLIED, b"test_data".as_slice())
                        .unwrap();
                }
                txn.commit().unwrap();
            }

            // Re-open: do the same write-txn that open() does (no inserts), then read.
            {
                let db = Database::create(&path).unwrap();
                {
                    let txn = db.begin_write().unwrap();
                    txn.open_table(SM_META_TABLE).unwrap();
                    txn.commit().unwrap();
                }
                let txn = db.begin_read().unwrap();
                let t = txn.open_table(SM_META_TABLE).unwrap();
                let val = t.get(KEY_SM_LAST_APPLIED).unwrap();
                assert!(
                    val.is_some(),
                    "data must survive reopen with empty write txn"
                );
                assert_eq!(
                    val.unwrap().value(),
                    b"test_data".as_slice(),
                    "data value must be intact"
                );
            }
        }

        #[test]
        fn test_validate_store_consistency_both_none() {
            validate_store_consistency(None, None).expect("none/none is consistent");
        }

        #[test]
        fn test_validate_store_consistency_no_purge() {
            let applied = Some(log_id(1, 5));
            validate_store_consistency(None, applied).expect("no purge, applied set: consistent");
        }

        #[test]
        fn test_validate_store_consistency_applied_ahead() {
            let purged = Some(log_id(1, 3));
            let applied = Some(log_id(1, 10));
            validate_store_consistency(purged, applied).expect("applied > purged: consistent");
        }

        #[test]
        fn test_validate_store_consistency_applied_equals_purged() {
            let lid = log_id(1, 5);
            validate_store_consistency(Some(lid), Some(lid))
                .expect("applied == purged: consistent");
        }

        #[test]
        fn test_validate_store_consistency_applied_behind_purged() {
            let purged = Some(log_id(1, 10));
            let applied = Some(log_id(1, 3));
            let err = validate_store_consistency(purged, applied)
                .expect_err("applied < purged: must be an error");
            assert!(
                err.to_string().contains("state machine is behind"),
                "error message must explain the skew: {err}"
            );
        }

        #[test]
        fn test_validate_store_consistency_purged_set_applied_none() {
            let purged = Some(log_id(1, 5));
            let err = validate_store_consistency(purged, None)
                .expect_err("purged set but applied None: must be an error");
            assert!(
                err.to_string()
                    .contains("state machine last_applied is None"),
                "error message must identify missing last_applied: {err}"
            );
        }
    }
}

#[cfg(feature = "distributed-raft")]
pub use raft_impl::{ArrowRedbLogStore, ArrowRedbStateMachine, validate_store_consistency};
