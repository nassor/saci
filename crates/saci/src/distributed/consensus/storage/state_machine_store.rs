//! Arrow-IPC Raft state machine over the application redb file.
//!
//! Holds [`ArrowRedbStateMachine`] and its [`RaftStateMachine`] implementation:
//! committed commands are applied through `sm_apply`, and `last_applied` /
//! `last_membership` are persisted into the same redb file as the application
//! data so a single fsync covers both.
//!
//! Named `state_machine_store` rather than `state_machine` so it does not
//! collide with the sibling [`crate::distributed::consensus::state_machine`]
//! module whose `apply` it drives.

use std::io;
use std::io::Cursor;
use std::sync::{Arc, Mutex};

use futures_util::StreamExt;
use openraft::EntryPayload;
use openraft::StoredMembership;
use openraft::storage::{EntryResponder, RaftStateMachine};
use openraft::type_config::alias::{LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use redb::{Database, ReadableDatabase};

use super::{SM_META_TABLE, dec, enc, join_to_io, to_io};
use crate::distributed::consensus::snapshot::raft_impl::{
    ArrowSnapshotBuilder, install_snapshot_bytes,
};
use crate::distributed::consensus::state_machine::{
    KEY_SM_LAST_APPLIED, KEY_SM_LAST_MEMBERSHIP, apply as sm_apply,
};
use crate::distributed::consensus::types::{ConsensusResponse, SaciTypeConfig};

/// State machine applying committed Arrow-IPC [`ConsensusCommand`](crate::distributed::consensus::ConsensusCommand) entries
/// to the redb application database.
///
/// Snapshot support uses Arrow IPC serialization via [`ArrowSnapshotBuilder`].
///
/// ## Persistence
///
/// `last_applied` and `last_membership` are persisted in the same redb
/// database as the application data, under the `arrow_sm_meta` table. This
/// makes the state machine restart-safe: `open()` restores these values so
/// openraft can skip re-applying already-committed log entries.
pub struct ArrowRedbStateMachine {
    pub(crate) db: Arc<Mutex<Database>>,
    last_applied: Option<LogIdOf<SaciTypeConfig>>,
    last_membership: StoredMembershipOf<SaciTypeConfig>,
}

impl ArrowRedbStateMachine {
    /// Open (or create) a state machine wrapping the given redb application
    /// database.
    ///
    /// Reads `last_applied` and `last_membership` from the persisted
    /// `arrow_sm_meta` table so restarts recover the correct watermark.
    /// Also ensures the `arrow_sm_meta` table exists (creates it on first
    /// open).
    pub fn open(db: Arc<Mutex<Database>>) -> io::Result<Self> {
        let (last_applied, last_membership) = {
            let guard = db.lock().unwrap();

            // Ensure the SM metadata table exists. redb requires a write txn to create
            // a table for the first time; the txn is a no-op once it exists.
            {
                let txn = guard.begin_write().map_err(to_io)?;
                txn.open_table(SM_META_TABLE).map_err(to_io)?;
                txn.commit().map_err(to_io)?;
            }

            let txn = guard.begin_read().map_err(to_io)?;
            let table = txn.open_table(SM_META_TABLE).map_err(to_io)?;

            let last_applied: Option<LogIdOf<SaciTypeConfig>> =
                match table.get(KEY_SM_LAST_APPLIED).map_err(to_io)? {
                    Some(v) => dec(v.value())?,
                    None => None,
                };

            let last_membership: StoredMembershipOf<SaciTypeConfig> =
                match table.get(KEY_SM_LAST_MEMBERSHIP).map_err(to_io)? {
                    Some(v) => dec(v.value())?,
                    None => StoredMembership::default(),
                };

            (last_applied, last_membership)
        };

        Ok(Self {
            db,
            last_applied,
            last_membership,
        })
    }

    /// Persist `last_applied` and `last_membership` to the app redb file in
    /// a single transaction (one fsync covers both fields).
    ///
    /// Used directly in tests to verify persistence without going through the
    /// async `apply` / `install_snapshot` paths. In production, the equivalent
    /// logic runs inside `spawn_blocking` at each call site.
    #[cfg(test)]
    fn persist_sm_meta(&self) -> io::Result<()> {
        let applied_bytes = enc(&self.last_applied)?;
        let membership_bytes = enc(&self.last_membership)?;

        let guard = self.db.lock().unwrap();
        let txn = guard.begin_write().map_err(to_io)?;
        {
            let mut table = txn.open_table(SM_META_TABLE).map_err(to_io)?;
            table
                .insert(KEY_SM_LAST_APPLIED, applied_bytes.as_slice())
                .map_err(to_io)?;
            table
                .insert(KEY_SM_LAST_MEMBERSHIP, membership_bytes.as_slice())
                .map_err(to_io)?;
        }
        txn.commit().map_err(to_io)
    }
}

impl RaftStateMachine<SaciTypeConfig> for ArrowRedbStateMachine {
    type SnapshotData = Cursor<Vec<u8>>;
    type SnapshotBuilder = ArrowSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogIdOf<SaciTypeConfig>>,
            StoredMembershipOf<SaciTypeConfig>,
        ),
        io::Error,
    > {
        Ok((self.last_applied, self.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, entries: Strm) -> Result<(), io::Error>
    where
        Strm: futures_util::Stream<Item = Result<EntryResponder<SaciTypeConfig>, io::Error>>
            + Unpin
            + Send,
    {
        let mut entries = entries;
        while let Some(item) = entries.next().await {
            let (entry, responder) = item?;
            let log_id = entry.log_id;
            match entry.payload {
                EntryPayload::Blank => {
                    // Blank entries carry no state, so advance immediately.
                    self.last_applied = Some(log_id);
                    if let Some(r) = responder {
                        r.send(ConsensusResponse::ClaimAcked);
                    }
                }
                EntryPayload::Normal(cmd) => {
                    // Propagate I/O errors out of sm_apply so openraft halts rather
                    // than silently skipping the entry. last_applied advances only
                    // after a successful apply, so a crash mid-apply cannot leave a
                    // false watermark.
                    let db = Arc::clone(&self.db);
                    let response = tokio::task::spawn_blocking(move || {
                        let db = db
                            .lock()
                            .map_err(|_| io::Error::other("db mutex poisoned"))?;
                        sm_apply(&db, cmd).map_err(|e| io::Error::other(format!("sm_apply: {e}")))
                    })
                    .await
                    .map_err(join_to_io)??;
                    self.last_applied = Some(log_id);
                    if let Some(r) = responder {
                        r.send(response);
                    }
                }
                EntryPayload::Membership(mem) => {
                    // Membership changes carry no I/O, so advance immediately.
                    self.last_applied = Some(log_id);
                    self.last_membership = StoredMembership::new(Some(log_id), mem.clone());
                    if let Some(r) = responder {
                        r.send(ConsensusResponse::ClaimAcked);
                    }
                }
            }
        }
        // Persist the watermarks after processing the full batch. A crash
        // here is safe: at-least-once semantics mean entries will be
        // re-applied after restart until the watermark advances past them.
        // Run inside spawn_blocking: this issues an fsync via redb commit.
        {
            let db = Arc::clone(&self.db);
            let last_applied = self.last_applied;
            let last_membership = self.last_membership.clone();
            tokio::task::spawn_blocking(move || {
                let applied_bytes = enc(&last_applied)?;
                let membership_bytes = enc(&last_membership)?;
                let guard = db
                    .lock()
                    .map_err(|_| io::Error::other("db mutex poisoned"))?;
                let txn = guard.begin_write().map_err(to_io)?;
                {
                    let mut table = txn.open_table(SM_META_TABLE).map_err(to_io)?;
                    table
                        .insert(KEY_SM_LAST_APPLIED, applied_bytes.as_slice())
                        .map_err(to_io)?;
                    table
                        .insert(KEY_SM_LAST_MEMBERSHIP, membership_bytes.as_slice())
                        .map_err(to_io)?;
                }
                txn.commit().map_err(to_io)
            })
            .await
            .map_err(join_to_io)??;
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        ArrowSnapshotBuilder {
            db: Arc::clone(&self.db),
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
        }
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<SaciTypeConfig>,
        snapshot: Self::SnapshotData,
    ) -> Result<(), io::Error> {
        let data = snapshot.into_inner();
        // Update in-memory state first so we can encode watermarks below.
        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();

        if !data.is_empty() {
            // Encode watermarks on this thread before entering spawn_blocking.
            let applied_bytes = enc(&self.last_applied)?;
            let membership_bytes = enc(&self.last_membership)?;

            // Install snapshot data and write watermarks in one WriteTransaction
            // so a crash cannot leave them split.
            let db = Arc::clone(&self.db);
            tokio::task::spawn_blocking(move || {
                let db = db
                    .lock()
                    .map_err(|_| io::Error::other("db mutex poisoned"))?;
                install_snapshot_bytes(
                    &db,
                    &data,
                    Some((applied_bytes.as_slice(), membership_bytes.as_slice())),
                )
                .map_err(|e| io::Error::other(format!("install_snapshot: {e}")))
            })
            .await
            .map_err(join_to_io)??;
        } else {
            // Empty snapshot: no data to install, but still persist watermarks.
            let db = Arc::clone(&self.db);
            let applied_bytes = enc(&self.last_applied)?;
            let membership_bytes = enc(&self.last_membership)?;
            tokio::task::spawn_blocking(move || {
                let guard = db
                    .lock()
                    .map_err(|_| io::Error::other("db mutex poisoned"))?;
                let txn = guard.begin_write().map_err(to_io)?;
                {
                    let mut table = txn.open_table(SM_META_TABLE).map_err(to_io)?;
                    table
                        .insert(KEY_SM_LAST_APPLIED, applied_bytes.as_slice())
                        .map_err(to_io)?;
                    table
                        .insert(KEY_SM_LAST_MEMBERSHIP, membership_bytes.as_slice())
                        .map_err(to_io)?;
                }
                txn.commit().map_err(to_io)
            })
            .await
            .map_err(join_to_io)??;
        }
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<SaciTypeConfig, Self::SnapshotData>>, io::Error> {
        // If nothing has been applied yet there is no snapshot to return.
        // openraft treats `None` as "no snapshot" and will send the full log
        // instead, which is correct for a freshly-initialized node.
        if self.last_applied.is_none() {
            return Ok(None);
        }

        // Move db lock + build_snapshot_bytes into spawn_blocking:
        // building the snapshot holds the Mutex and may issue disk reads.
        let db = Arc::clone(&self.db);
        let payload = tokio::task::spawn_blocking(move || {
            let db = db
                .lock()
                .map_err(|_| io::Error::other("db mutex poisoned"))?;
            crate::distributed::consensus::snapshot::raft_impl::build_snapshot_bytes(&db)
                .map_err(|e| io::Error::other(format!("get_current_snapshot build: {e}")))
        })
        .await
        .map_err(join_to_io)??;

        use openraft::{Snapshot, SnapshotMeta};
        use std::io::Cursor;

        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.last_membership.clone(),
        };
        Ok(Some(Snapshot {
            meta,
            snapshot: Cursor::new(payload),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::log_id;
    use super::*;
    use openraft::entry::RaftEntry;
    use openraft::type_config::alias::EntryOf;
    use tempfile::TempDir;

    /// The `last_applied` index written by `persist_sm_meta` survives a `Database`
    /// close and reopen.
    #[tokio::test]
    async fn test_persist_sm_meta_survives_reopen() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("meta_persist_test.redb");

        // Write via open() + persist_sm_meta directly.
        {
            let app_db = Arc::new(Mutex::new(Database::create(&db_path).unwrap()));
            let mut sm = ArrowRedbStateMachine::open(Arc::clone(&app_db)).unwrap();
            sm.last_applied = Some(log_id(2, 10));
            sm.persist_sm_meta().unwrap();
        }

        let app_db2 = Arc::new(Mutex::new(Database::create(&db_path).unwrap()));
        let sm2 = ArrowRedbStateMachine::open(app_db2).unwrap();
        assert_eq!(
            sm2.last_applied.map(|l| l.index),
            Some(10),
            "last_applied must persist across reopen"
        );
    }

    /// With `D = ConsensusCommand` on `SaciTypeConfig`, the apply path carries the
    /// application command directly, with no string encode/decode step. An applied
    /// `Normal` entry advances `last_applied` and produces the corresponding row in the
    /// application redb file. No openraft responder is passed, since constructing one
    /// needs crate-internal types; the invariant checked here is that `last_applied`
    /// advances and the row is readable afterwards.
    #[tokio::test]
    async fn test_state_machine_apply_advances_last_applied_on_success() {
        use crate::distributed::consensus::state_machine::read_master_batch;
        use crate::distributed::consensus::types::ConsensusCommand;
        use futures_util::stream;
        use openraft::Entry;

        let dir = TempDir::new().unwrap();
        let app_db = Arc::new(Mutex::new(
            Database::create(dir.path().join("sm_apply_app.redb")).unwrap(),
        ));
        let mut sm = ArrowRedbStateMachine::open(Arc::clone(&app_db)).unwrap();
        assert!(sm.last_applied.is_none(), "fresh SM: last_applied is None");

        // A valid RegisterMasterBatch command entry. With `D = ConsensusCommand` the
        // command goes straight into `EntryPayload::Normal`, with no string encoding.
        let cmd = ConsensusCommand::RegisterMasterBatch {
            batch_id: 42,
            component: "task3".to_string(),
            schema_id: 1,
            ipc_bytes: vec![0u8; 32],
            total_rows: 10,
            now_at_propose: 0,
        };

        let lid = log_id(1, 5);
        let mut entry: EntryOf<SaciTypeConfig> = Entry::new_blank(lid);
        entry.payload = openraft::EntryPayload::Normal(cmd);

        // No responder, as in a follower-side apply with no client waiting.
        let stream = stream::iter(vec![Ok((entry, None))]);
        sm.apply(stream).await.unwrap();

        // After apply, last_applied must have advanced to the entry's log_id. The
        // order is deliberate: advance only after the state machine side effect
        // succeeds.
        assert_eq!(
            sm.last_applied.map(|l| l.index),
            Some(5),
            "last_applied must advance after successful apply"
        );

        // Verify the state-machine side effect took place, so "applied" stays
        // distinguishable from "skipped".
        let db = app_db.lock().unwrap();
        let record = read_master_batch(&db, 42)
            .unwrap()
            .expect("master batch 42 must exist after apply");
        assert_eq!(record.component, "task3");
        assert_eq!(record.total_rows, 10);
    }

    /// After apply, `last_applied` and `last_membership` are written to
    /// redb. Re-opening the state machine with the same database must
    /// restore those values so openraft does not re-apply already-committed
    /// entries on restart.
    #[tokio::test]
    async fn test_state_machine_restart_restores_watermarks() {
        use crate::distributed::consensus::types::ConsensusCommand;
        use futures_util::stream;
        use openraft::Entry;

        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("restart_test_app.redb");

        let last_applied_index = {
            let app_db = Arc::new(Mutex::new(Database::create(&db_path).unwrap()));
            let mut sm = ArrowRedbStateMachine::open(Arc::clone(&app_db)).unwrap();

            let cmd = ConsensusCommand::RegisterMasterBatch {
                batch_id: 7,
                component: "restart_comp".to_string(),
                schema_id: 1,
                ipc_bytes: vec![0u8; 32],
                total_rows: 5,
                now_at_propose: 0,
            };
            let lid = log_id(2, 10);
            let mut entry: EntryOf<SaciTypeConfig> = Entry::new_blank(lid);
            entry.payload = openraft::EntryPayload::Normal(cmd);

            let s = stream::iter(vec![Ok((entry, None))]);
            sm.apply(s).await.unwrap();

            sm.last_applied.unwrap().index
        };
        // The `app_db` Arc (and the Mutex<Database>) is dropped here,
        // releasing the redb file lock before the next open.

        // Re-open the same file, as a restart would.
        let app_db2 = Arc::new(Mutex::new(Database::create(&db_path).unwrap()));
        let sm2 = ArrowRedbStateMachine::open(Arc::clone(&app_db2)).unwrap();

        assert_eq!(
            sm2.last_applied.map(|l| l.index),
            Some(last_applied_index),
            "last_applied must be restored after restart"
        );
    }

    /// `applied_state()` must return the persisted `last_applied` log-id and
    /// `last_membership` after a restart, not a fresh empty state.
    ///
    /// Applies a `Normal` entry (advancing `last_applied`) and a `Membership` entry
    /// (advancing `last_membership` to a non-default value), closes the database,
    /// reopens it, then checks the public `applied_state()` trait method.
    #[tokio::test]
    async fn test_applied_state_returns_persisted_values_after_restart() {
        use futures_util::stream;
        use openraft::{Entry, Membership};
        use std::collections::BTreeSet;

        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("applied_state_restart.redb");

        let (expected_log_index, expected_voter_ids) = {
            let app_db = Arc::new(Mutex::new(Database::create(&db_path).unwrap()));
            let mut sm = ArrowRedbStateMachine::open(Arc::clone(&app_db)).unwrap();

            // Normal entry: advances last_applied to index 3.
            let mut normal_entry: EntryOf<SaciTypeConfig> = Entry::new_blank(log_id(1, 3));
            normal_entry.payload = openraft::EntryPayload::Normal(
                crate::distributed::consensus::types::ConsensusCommand::AckClaim {
                    claim_id: uuid::Uuid::nil(),
                    instance_id: uuid::Uuid::nil(),
                },
            );

            // Membership entry: advances last_membership to {1, 2}.
            let voter_ids: BTreeSet<u64> = [1u64, 2u64].into_iter().collect();
            // `new_with_defaults` expects `IntoIterator<Item = NID>` for the
            // nodes argument and fills in `N::default()` for each node entry.
            let membership =
                Membership::new_with_defaults(vec![voter_ids.clone()], voter_ids.iter().copied());
            let mem_entry: EntryOf<SaciTypeConfig> = Entry {
                log_id: log_id(1, 4),
                payload: openraft::EntryPayload::Membership(membership),
            };

            sm.apply(stream::iter(vec![
                Ok((normal_entry, None)),
                Ok((mem_entry, None)),
            ]))
            .await
            .unwrap();

            (sm.last_applied.unwrap().index, voter_ids)
            // app_db Arc dropped here → file lock released.
        };

        // Reopen, as a process restart would.
        let app_db2 = Arc::new(Mutex::new(Database::create(&db_path).unwrap()));
        let mut sm2 = ArrowRedbStateMachine::open(Arc::clone(&app_db2)).unwrap();

        let (restored_log_id, restored_membership) = sm2.applied_state().await.unwrap();

        assert_eq!(
            restored_log_id.map(|l| l.index),
            Some(expected_log_index),
            "applied_state() must return persisted last_applied after restart"
        );
        assert_eq!(
            restored_membership
                .membership()
                .voter_ids()
                .collect::<BTreeSet<u64>>(),
            expected_voter_ids,
            "applied_state() must return persisted last_membership after restart"
        );
    }

    /// `get_current_snapshot` must return `None` on a fresh (never-applied)
    /// state machine and a real snapshot after at least one entry is applied.
    #[tokio::test]
    async fn test_get_current_snapshot_returns_snapshot_after_apply() {
        use crate::distributed::consensus::types::ConsensusCommand;
        use futures_util::stream;
        use openraft::Entry;

        let dir = TempDir::new().unwrap();
        let app_db = Arc::new(Mutex::new(
            Database::create(dir.path().join("snap_test.redb")).unwrap(),
        ));
        let mut sm = ArrowRedbStateMachine::open(Arc::clone(&app_db)).unwrap();

        // Before any apply: no snapshot.
        let snap = sm.get_current_snapshot().await.unwrap();
        assert!(snap.is_none(), "fresh SM must return None snapshot");

        // Apply one entry.
        let cmd = ConsensusCommand::RegisterMasterBatch {
            batch_id: 99,
            component: "snap_comp".to_string(),
            schema_id: 1,
            ipc_bytes: vec![0u8; 32],
            total_rows: 3,
            now_at_propose: 0,
        };
        let lid = log_id(1, 1);
        let mut entry: EntryOf<SaciTypeConfig> = Entry::new_blank(lid);
        entry.payload = openraft::EntryPayload::Normal(cmd);
        sm.apply(stream::iter(vec![Ok((entry, None))]))
            .await
            .unwrap();

        // After apply: snapshot must exist with matching metadata.
        let snap = sm.get_current_snapshot().await.unwrap();
        assert!(snap.is_some(), "SM must return a snapshot after apply");
        let snap = snap.unwrap();
        assert_eq!(
            snap.meta.last_log_id.map(|l| l.index),
            Some(1),
            "snapshot last_log_id must match last applied"
        );
        assert!(
            !snap.snapshot.into_inner().is_empty(),
            "snapshot payload must be non-empty"
        );
    }

    /// A stream-level error propagates out of `apply` and leaves `last_applied` at its
    /// pre-apply value of `None`, while a successful apply advances it.
    #[tokio::test]
    async fn sm_apply_err_halts_stream() {
        let dir = TempDir::new().unwrap();
        let app_db = Arc::new(Mutex::new(
            Database::create(dir.path().join("sm_test.redb")).unwrap(),
        ));
        let mut sm = ArrowRedbStateMachine::open(Arc::clone(&app_db)).unwrap();

        assert!(
            sm.last_applied.is_none(),
            "initial last_applied must be None"
        );

        // Inject a stream-level Err (simulates network / IO failure delivering entries).
        let stream_err: io::Error = io::Error::other("injected stream failure");
        let stream = futures_util::stream::iter(vec![Err(stream_err)]);
        let result = sm.apply(stream).await;
        assert!(result.is_err(), "apply must propagate stream Err");
        assert!(
            sm.last_applied.is_none(),
            "last_applied must not advance when stream returns Err"
        );
    }
}
