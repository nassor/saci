//! Raft consensus fault-injection and chaos tests.
//!
//! Two layers. `mod unit` drives the state machine and its redb storage
//! directly, with no cluster and no Docker: apply idempotency, renewal
//! monotonicity, the reclaim sweep, and snapshot atomicity. Everything below
//! it is Docker-gated, driving a real multi-node cluster through the
//! `RaftClusterHarness` in `tests/common` and soft-skipping when no daemon is
//! reachable.
//!
//! The cluster invariants under test are election safety and log convergence:
//! a partitioned majority elects, a minority does not, a lagging follower
//! catches up, and no node keeps a divergent log after a fault heals.

#![cfg(feature = "distributed-raft")]

mod common;

// These tests exercise the state machine and storage directly, with no running
// Raft cluster and no Docker.

#[cfg(test)]
mod unit {
    use redb::Database;
    use saci_service::distributed::consensus::state_machine::{
        apply, dump_state, read_claim, read_master_batch, restore_state,
    };
    use saci_service::distributed::consensus::types::{
        ClaimStatus, ConsensusCommand, ConsensusResponse,
    };
    use tempfile::NamedTempFile;

    fn temp_db() -> (Database, tempfile::TempPath) {
        let file = NamedTempFile::new().expect("tempfile");
        let path = file.into_temp_path();
        let db = Database::create(&path).expect("redb create");
        (db, path)
    }

    fn small_ipc() -> Vec<u8> {
        vec![0xAB; 64]
    }

    fn register_batch(db: &Database, batch_id: u64, total_rows: u32) {
        apply(
            db,
            ConsensusCommand::RegisterMasterBatch {
                batch_id,
                component: format!("comp_{batch_id}"),
                schema_id: 1,
                ipc_bytes: small_ipc(),
                total_rows,
                now_at_propose: 0,
            },
        )
        .unwrap();
    }

    fn claim_range(
        db: &Database,
        batch_id: u64,
        start: u32,
        end: u32,
        claim_id: uuid::Uuid,
        now: u64,
        ttl: u64,
    ) -> ConsensusResponse {
        apply(
            db,
            ConsensusCommand::ClaimRowRange {
                batch_id,
                row_range_start: start,
                row_range_end: end,
                claim_id,
                instance_id: uuid::Uuid::now_v7(),
                lease_ttl_millis: ttl,
                now_at_propose: now,
            },
        )
        .unwrap()
    }

    // ClaimRowRange must be idempotent on replay.
    #[test]
    fn claim_row_range_replay_idempotent() {
        let (db, _p) = temp_db();
        register_batch(&db, 1, 100);

        let claim_id = uuid::Uuid::now_v7();
        let cmd = ConsensusCommand::ClaimRowRange {
            batch_id: 1,
            row_range_start: 0,
            row_range_end: 50,
            claim_id,
            instance_id: uuid::Uuid::now_v7(),
            lease_ttl_millis: 30_000,
            now_at_propose: 1_000,
        };

        let r1 = apply(&db, cmd.clone()).unwrap();
        assert!(
            matches!(r1, ConsensusResponse::BatchClaimed { .. }),
            "{r1:?}"
        );

        // Replay must not return an error.
        let r2 = apply(&db, cmd).unwrap();
        assert!(
            matches!(
                r2,
                ConsensusResponse::BatchClaimed {
                    row_range_start: 0,
                    row_range_end: 50,
                    ..
                }
            ),
            "replay must be idempotent, got: {r2:?}"
        );

        // Exactly one claim record.
        let rec = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(rec.status, ClaimStatus::Claimed);
    }

    // Checkpoint must be idempotent on replay: checkpoint_seq increments once.
    #[test]
    fn checkpoint_replay_idempotent() {
        let (db, _p) = temp_db();
        register_batch(&db, 1, 100);
        let claim_id = uuid::Uuid::now_v7();
        claim_range(&db, 1, 0, 100, claim_id, 0, 30_000);

        let cp_cmd = ConsensusCommand::Checkpoint {
            claim_id,
            stage_idx: 0,
            ipc_bytes: vec![0xCA, 0xFE],
            schema_id: 1,
            now_at_propose: 42,
        };

        let r1 = apply(&db, cp_cmd.clone()).unwrap();
        let ConsensusResponse::CheckpointWritten {
            checkpoint_id: seq1,
        } = r1
        else {
            panic!("expected CheckpointWritten: {r1:?}");
        };

        let r2 = apply(&db, cp_cmd).unwrap();
        let ConsensusResponse::CheckpointWritten {
            checkpoint_id: seq2,
        } = r2
        else {
            panic!("expected CheckpointWritten on replay: {r2:?}");
        };

        assert_eq!(
            seq2, seq1,
            "checkpoint_seq must not double-increment on replay"
        );
        let batch = read_master_batch(&db, 1).unwrap().unwrap();
        assert_eq!(batch.checkpoint_seq, seq1);
    }

    // RenewClaim must be monotonic: a stale now_at_propose cannot move expiry back.
    #[test]
    fn renew_claim_monotonic() {
        let (db, _p) = temp_db();
        register_batch(&db, 1, 100);
        let claim_id = uuid::Uuid::now_v7();
        let inst = uuid::Uuid::now_v7();
        // Claim at t=1000, ttl=60_000 → expires_at=61_000.
        apply(
            &db,
            ConsensusCommand::ClaimRowRange {
                batch_id: 1,
                row_range_start: 0,
                row_range_end: 100,
                claim_id,
                instance_id: inst,
                lease_ttl_millis: 60_000,
                now_at_propose: 1_000,
            },
        )
        .unwrap();

        // Renew with stale now=500: new_expires=60_500 < 61_000 → max() keeps 61_000.
        let resp = apply(
            &db,
            ConsensusCommand::RenewClaim {
                claim_id,
                instance_id: inst,
                lease_ttl_millis: 60_000,
                now_at_propose: 500,
            },
        )
        .unwrap();
        match resp {
            ConsensusResponse::ClaimRenewed { expires_at } => {
                assert_eq!(expires_at, 61_000, "stale renew must not regress expiry");
            }
            other => panic!("unexpected: {other:?}"),
        }

        let rec = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(rec.lease_expires_at, 61_000);
    }

    // ReclaimExpired sweeps Claimed → Pending for expired claims.
    #[test]
    fn reclaim_expired_frees_ranges() {
        let (db, _p) = temp_db();
        register_batch(&db, 1, 100);
        let claim_id = uuid::Uuid::now_v7();
        // Claim at t=0, ttl=100 → expires_at=100.
        claim_range(&db, 1, 0, 100, claim_id, 0, 100);

        // Before expiry: nothing reclaimed.
        let r1 = apply(&db, ConsensusCommand::ReclaimExpired { now_at_propose: 50 }).unwrap();
        assert!(
            matches!(
                r1,
                ConsensusResponse::ExpiredReclaimed { reclaimed_count: 0 }
            ),
            "{r1:?}"
        );
        let rec = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(rec.status, ClaimStatus::Claimed);

        // After expiry: claim freed.
        let r2 = apply(
            &db,
            ConsensusCommand::ReclaimExpired {
                now_at_propose: 200,
            },
        )
        .unwrap();
        assert!(
            matches!(
                r2,
                ConsensusResponse::ExpiredReclaimed { reclaimed_count: 1 }
            ),
            "{r2:?}"
        );
        let rec = read_claim(&db, claim_id).unwrap().unwrap();
        assert_eq!(rec.status, ClaimStatus::Pending);
        assert_eq!(rec.lease_expires_at, 0);

        // The range must now be claimable.
        let claim_id2 = uuid::Uuid::now_v7();
        let r3 = claim_range(&db, 1, 0, 100, claim_id2, 200, 30_000);
        assert!(
            matches!(r3, ConsensusResponse::BatchClaimed { .. }),
            "range should be reclaimable after expiry sweep: {r3:?}"
        );
    }

    // Snapshot install purges all pre-existing state.
    #[test]
    fn install_snapshot_atomic_clear() {
        // db1 has batch 1 + claim c1.
        let (db1, _p1) = temp_db();
        register_batch(&db1, 1, 100);
        let c1 = uuid::Uuid::now_v7();
        claim_range(&db1, 1, 0, 50, c1, 0, 30_000);

        // db2 has batch 3 + claim c4 (old state that must be replaced).
        let (db2, _p2) = temp_db();
        register_batch(&db2, 3, 50);
        let c4 = uuid::Uuid::now_v7();
        claim_range(&db2, 3, 0, 50, c4, 0, 30_000);

        // Restore db1 snapshot into db2.
        let (batches, claims, checkpoints, instances) = dump_state(&db1).unwrap();
        restore_state(&db2, batches, claims, checkpoints, instances, None).unwrap();

        // db2 must contain exactly db1's state.
        assert!(
            read_master_batch(&db2, 1).unwrap().is_some(),
            "batch 1 must be present"
        );
        assert!(
            read_master_batch(&db2, 3).unwrap().is_none(),
            "old batch 3 must be purged"
        );
        assert!(
            read_claim(&db2, c1).unwrap().is_some(),
            "claim c1 must be present"
        );
        assert!(
            read_claim(&db2, c4).unwrap().is_none(),
            "old claim c4 must be purged"
        );
    }

    // Snapshot magic, version, and CRC-32 are validated on install.
    #[test]
    fn snapshot_format_magic_version_crc() {
        use saci_service::distributed::consensus::snapshot::{
            build_snapshot_bytes, install_snapshot_bytes,
        };

        let (db, _p) = temp_db();
        let snap = build_snapshot_bytes(&db).unwrap();
        assert!(
            snap.len() >= 16,
            "snapshot must have at least 16-byte header"
        );

        assert_eq!(&snap[..8], b"ARROWSNA", "magic bytes must match");

        let version = u32::from_le_bytes(snap[8..12].try_into().unwrap());
        assert_eq!(version, 2, "snapshot version must be 2");

        // Valid snapshot installs cleanly.
        let (db2, _p2) = temp_db();
        install_snapshot_bytes(&db2, &snap, None).unwrap();

        // Tampered magic → rejected.
        let mut bad_magic = snap.clone();
        bad_magic[0] ^= 0xFF;
        let (db3, _p3) = temp_db();
        assert!(
            install_snapshot_bytes(&db3, &bad_magic, None).is_err(),
            "tampered magic must be rejected"
        );

        // Tampered body → CRC mismatch.
        let mut bad_body = snap.clone();
        if bad_body.len() > 16 {
            bad_body[16] ^= 0xFF;
        }
        let (db4, _p4) = temp_db();
        assert!(
            install_snapshot_bytes(&db4, &bad_body, None).is_err(),
            "tampered body (CRC mismatch) must be rejected"
        );
    }
}

// End-to-end invariants through the public surface only: `single_node`,
// `register_master_batch`, `claim_next_batch`, and `propose_reclaim_expired`.

#[cfg(test)]
mod idempotency {
    use saci_service::distributed::consensus::store::RedbSharedStore;
    use saci_service::distributed::partition::PartitionSource;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::NamedTempFile;
    use uuid::Uuid;

    fn temp_path() -> PathBuf {
        let file = NamedTempFile::new().expect("tempfile");
        let path = file.into_temp_path();
        path.to_path_buf()
    }

    fn small_ipc() -> Vec<u8> {
        vec![0xAB; 64]
    }

    fn now_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    /// Register a batch then claim it twice. The second `claim_next_batch` must
    /// return `Ok(None)`: the range is already claimed and must not be issued to a
    /// second runner.
    #[tokio::test]
    async fn end_to_end_claim_replay_via_raft() {
        let path = temp_path();
        let store = RedbSharedStore::single_node(&path).unwrap();

        store
            .register_master_batch(1, "comp".to_string(), 1, small_ipc(), 100)
            .await
            .unwrap();

        let instance_id = Uuid::now_v7();
        let claim1 = store
            .claim_next_batch(instance_id)
            .await
            .unwrap()
            .expect("first claim must succeed");
        assert_eq!(claim1.batch_id, 1);

        // Second caller with a different instance sees no available batch.
        let claim2 = store.claim_next_batch(Uuid::now_v7()).await.unwrap();
        assert!(
            claim2.is_none(),
            "already-claimed range must not be re-issued: got {claim2:?}"
        );
    }

    /// Claim a batch with a short TTL, sleep past expiry, sweep with
    /// `propose_reclaim_expired`, and verify the range is re-claimable.
    #[tokio::test]
    async fn reclaim_expired_sweep_via_store() {
        let path = temp_path();
        let store = RedbSharedStore::single_node(&path)
            .unwrap()
            .with_lease_ttl_millis(50); // 50 ms TTL

        store
            .register_master_batch(2, "comp".to_string(), 1, small_ipc(), 100)
            .await
            .unwrap();

        let instance_id = Uuid::now_v7();
        let claim = store
            .claim_next_batch(instance_id)
            .await
            .unwrap()
            .expect("initial claim must succeed");
        assert_eq!(claim.batch_id, 2);

        // Wait for the lease to expire.
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;

        // Sweep with a timestamp well past expiry.
        let reclaimed = store.propose_reclaim_expired(now_millis()).await.unwrap();
        assert_eq!(reclaimed, 1, "exactly one expired claim must be freed");

        // The range is pending again, so a new runner can claim it.
        let reclaim = store.claim_next_batch(Uuid::now_v7()).await.unwrap();
        assert!(
            reclaim.is_some(),
            "reclaimed range must be claimable again after sweep"
        );
    }
}

use common::RaftClusterHarness;
use std::time::Duration;

/// After cutting all inbound and outbound links for the leader, the remaining
/// two nodes must elect a new leader on a higher term.
#[tokio::test]
async fn isolate_leader_elects_new() -> anyhow::Result<()> {
    let Some(harness) = RaftClusterHarness::try_start(3).await else {
        return Ok(());
    };
    let leader_id = harness.await_leader().await?;
    let term_before = harness.max_term();

    harness.isolate_node((leader_id - 1) as usize)?;

    // The isolated node keeps reporting itself as leader until its own
    // leader lease expires, so its stale self-report must be excluded:
    // `await_leader` returns whichever node answers first, and when the
    // isolated leader is `nodes[0]` that self-report would satisfy the poll
    // meanwhile. Still tolerate windows with no leader at all.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let new_leader = loop {
        if let Ok(candidate) = harness.await_leader_excluding(leader_id).await {
            break candidate;
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "a different node must become leader after isolation: {}",
            harness.diagnostics()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_ne!(new_leader, leader_id);
    assert!(
        harness.max_term() > term_before,
        "a new election must advance the term past {term_before}"
    );

    // Heal, then the old leader must adopt the new leader's log.
    harness.toxiproxy().reset()?;
    harness.await_convergence(Duration::from_secs(30)).await?;

    harness.shutdown().await;
    Ok(())
}

/// Isolating a single follower (minority) must not cost the leader its term:
/// the remaining quorum keeps it in office, and the follower reconverges after
/// healing.
#[tokio::test]
async fn minority_partition_no_divergence() -> anyhow::Result<()> {
    let Some(harness) = RaftClusterHarness::try_start(3).await else {
        return Ok(());
    };
    let leader_id = harness.await_leader().await?;
    harness
        .await_applied_at_least(leader_id, 1, Duration::from_secs(10))
        .await?;
    let term_before = harness.max_term();

    // Pick a follower (node_id ∈ {1,2,3}, different from leader).
    let follower_id = (1u64..=3).find(|&id| id != leader_id).unwrap();
    harness.isolate_node((follower_id - 1) as usize)?;

    // Two of three nodes still form a quorum, so leadership must not move.
    // The isolated follower campaigns while cut off and raises its own term,
    // so `max_term` across all nodes is expected to climb; what must hold is
    // that the original leader is still the leader.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        harness.await_leader().await?,
        leader_id,
        "a minority partition must not unseat the leader: {}",
        harness.diagnostics()
    );
    let _ = term_before;

    // Heal the follower; every node must agree on one applied index.
    harness.toxiproxy().reset()?;
    harness.await_convergence(Duration::from_secs(30)).await?;

    harness.shutdown().await;
    Ok(())
}

/// 200 ms of latency on every link inbound to one follower must not desync it:
/// once the latency clears, its applied index matches the rest of the cluster.
#[tokio::test]
async fn lagging_follower_catches_up() -> anyhow::Result<()> {
    let Some(harness) = RaftClusterHarness::try_start(3).await else {
        return Ok(());
    };
    let leader_id = harness.await_leader().await?;

    let follower_id = (1u64..=3).find(|&id| id != leader_id).unwrap();
    let follower_idx = (follower_id - 1) as usize;

    // Add 200 ms latency on all links inbound to the follower so it lags.
    for peer in 0..3 {
        if peer == follower_idx {
            continue;
        }
        harness
            .toxiproxy()
            .add_latency(&RaftClusterHarness::proxy_name(peer, follower_idx), 200)?;
    }
    tokio::time::sleep(Duration::from_secs(3)).await;

    harness.toxiproxy().reset()?;
    harness.await_convergence(Duration::from_secs(30)).await?;

    harness.shutdown().await;
    Ok(())
}

/// A healthy 3-node cluster settles: one leader, a non-zero applied index on
/// every node, and the same index everywhere.
#[tokio::test]
async fn healthy_cluster_converges_on_one_leader() -> anyhow::Result<()> {
    let Some(harness) = RaftClusterHarness::try_start(3).await else {
        return Ok(());
    };
    let leader_id = harness.await_leader().await?;

    // `await_convergence` is the all-nodes-agree assertion: it only returns
    // once every node reports the same applied index. Re-reading the per-node
    // indices afterwards would race a further commit, so the returned snapshot
    // is the assertion.
    let converged = harness.await_convergence(Duration::from_secs(20)).await?;
    assert!(
        converged >= 1,
        "the leader's own term entry must be applied everywhere, got {converged}"
    );
    assert!(
        (1..=3).contains(&leader_id),
        "the elected leader must be one of the three nodes, got {leader_id}"
    );

    harness.shutdown().await;
    Ok(())
}

/// TCP RST injection between two nodes must not cause divergence: the cluster
/// stays available and the logs converge once the fault clears.
#[tokio::test]
async fn tcp_rst_does_not_cause_divergence() -> anyhow::Result<()> {
    let Some(harness) = RaftClusterHarness::try_start(3).await else {
        return Ok(());
    };
    let leader_id = harness.await_leader().await?;

    // Inject TCP RST on the leader→follower direction.
    let follower_id = (1u64..=3).find(|&id| id != leader_id).unwrap();
    let leader_idx = (leader_id - 1) as usize;
    let follower_idx = (follower_id - 1) as usize;
    harness.toxiproxy().add_reset_peer(
        &RaftClusterHarness::proxy_name(leader_idx, follower_idx),
        100,
    )?;

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Remove the fault and let the cluster heal. The budget covers the
    // leader's own recovery, not just the network's: a reset_peer toxic lets
    // the connect through and kills the stream after it, so the RST storm
    // reaches the leader's per-peer circuit-open threshold, every send to the
    // follower fast-fails for the 2s window, and the next real attempt waits
    // out the openraft backoff step that fast-fail consumed, up to 12s more.
    // The follower cannot shortcut that: one entry behind, raft's up-to-date
    // vote restriction blocks it from winning an election, so it catches up
    // only when a send from the leader gets through. The rest of the 60s is
    // room for the re-election a mid-storm step-down costs.
    harness.toxiproxy().reset()?;
    harness.await_convergence(Duration::from_secs(60)).await?;

    harness.shutdown().await;
    Ok(())
}
