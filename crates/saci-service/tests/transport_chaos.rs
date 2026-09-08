//! Chaos tests for the SACI Raft TCP transport layer. Each test soft-skips
//! when Docker is unavailable, so `cargo test` is safe without a daemon.
//!
//! Nothing is proposed into the SACI raft, so log progress here is raft's own:
//! a node appends one entry per term when it takes office. The properties
//! under test are that the transport keeps a slow-but-alive link up, recovers
//! a broken connection, survives truncated frames, and lets a partitioned
//! majority elect and reconverge.

#![cfg(feature = "distributed-raft")]

mod common;

use std::time::Duration;
use tokio::time::Instant;

/// Assert that the cluster elects a leader within `deadline`.
async fn await_leader_within(
    harness: &common::RaftClusterHarness,
    deadline: Duration,
) -> anyhow::Result<u64> {
    let timeout_at = Instant::now() + deadline;
    loop {
        match harness.await_leader().await {
            Ok(id) => return Ok(id),
            Err(_) if Instant::now() < timeout_at => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// With 150ms latency on every peer link a 3-node cluster still elects a leader
/// and holds it for at least 10 seconds: the transport's write timeout and the
/// server's idle-read timeout must not close connections that are slow but
/// alive.
///
/// The latency has to stay under the harness's 400ms election timeout. A delay
/// longer than that makes every vote arrive after the voter has already moved
/// to the next term, so the cluster splits votes forever — a property of the
/// configured timings, not of the transport.
#[tokio::test]
async fn latency_under_election_timeout_no_heartbeat_thrash() -> anyhow::Result<()> {
    let Some(harness) = common::RaftClusterHarness::try_start(3).await else {
        return Ok(());
    };
    let toxi = harness.toxiproxy();

    // Apply 500ms latency on all directed edges.
    for src in 0..3 {
        for dst in 0..3 {
            if src != dst {
                toxi.add_latency(&common::RaftClusterHarness::proxy_name(src, dst), 150)?;
            }
        }
    }

    let first_leader = await_leader_within(&harness, Duration::from_secs(15)).await?;

    let start = Instant::now();
    let mut changes = 0u32;
    let mut prev_leader = first_leader;
    while start.elapsed() < Duration::from_secs(10) {
        tokio::time::sleep(Duration::from_millis(500)).await;
        if let Ok(current) = harness.await_leader().await
            && current != prev_leader
        {
            changes += 1;
            prev_leader = current;
        }
    }

    assert!(
        changes <= 1,
        "too many leader changes under 150ms latency: {changes}"
    );

    harness.shutdown().await;
    Ok(())
}

/// A `reset_peer` toxic on the leader→follower link must not strand the
/// follower. The connection pool drops the broken stream and reconnects on the
/// next heartbeat, so the follower reconverges on the leader's applied index
/// once the toxic clears.
#[tokio::test]
async fn reset_peer_mid_replication_reconnects() -> anyhow::Result<()> {
    let Some(harness) = common::RaftClusterHarness::try_start(3).await else {
        return Ok(());
    };

    let leader_id = await_leader_within(&harness, Duration::from_secs(10)).await?;
    let leader_idx = (leader_id - 1) as usize;

    harness
        .await_applied_at_least(leader_id, 1, Duration::from_secs(10))
        .await?;

    // Inject reset_peer on the leader→first-follower edge.
    let follower = if leader_idx == 0 { 1 } else { 0 };
    let proxy = common::RaftClusterHarness::proxy_name(leader_idx, follower);
    harness.toxiproxy().add_reset_peer(&proxy, 0)?;

    tokio::time::sleep(Duration::from_millis(500)).await;
    harness.toxiproxy().delete_toxic(&proxy, "reset_peer")?;

    // A reconnect plus at most one re-election must leave every node agreeing.
    harness.await_convergence(Duration::from_secs(20)).await?;

    harness.shutdown().await;
    Ok(())
}

/// A 1 kbps bandwidth limit on the replication link must not hang the
/// follower's catch-up: the write-side timeout has to tolerate a slow pipe, so
/// the two nodes still converge once the limit clears.
#[tokio::test]
async fn bandwidth_1kbps_follower_still_converges() -> anyhow::Result<()> {
    let Some(harness) = common::RaftClusterHarness::try_start(2).await else {
        return Ok(());
    };
    let toxi = harness.toxiproxy();

    let leader_id = await_leader_within(&harness, Duration::from_secs(10)).await?;
    let leader_idx = (leader_id - 1) as usize;
    let follower_idx = 1 - leader_idx;

    // Apply a 1 kbps bandwidth limit on the leader→follower link.
    let proxy = common::RaftClusterHarness::proxy_name(leader_idx, follower_idx);
    toxi.add_bandwidth(&proxy, 1)?;

    // Hold the constraint long enough for several heartbeat intervals to be
    // squeezed through it, then release.
    tokio::time::sleep(Duration::from_secs(5)).await;
    toxi.reset()?;

    harness.await_convergence(Duration::from_secs(60)).await?;

    harness.shutdown().await;
    Ok(())
}

/// Disabling every proxy link to and from the leader must let the majority elect a
/// new leader. Re-enabling them must let the old leader rejoin as a follower with
/// no split-brain and no crash.
#[tokio::test]
async fn bidi_partition_elects_and_rejoins() -> anyhow::Result<()> {
    let Some(harness) = common::RaftClusterHarness::try_start(3).await else {
        return Ok(());
    };
    let toxi = harness.toxiproxy();

    let first_leader = await_leader_within(&harness, Duration::from_secs(10)).await?;
    let leader_idx = (first_leader - 1) as usize;
    let term_before = harness.max_term();

    // Disable all links to/from the leader (bidi partition).
    for peer in 0..3 {
        if peer == leader_idx {
            continue;
        }
        toxi.disable_proxy(&common::RaftClusterHarness::proxy_name(leader_idx, peer))?;
        toxi.disable_proxy(&common::RaftClusterHarness::proxy_name(peer, leader_idx))?;
    }

    // The two remaining nodes hold the majority. The isolated old leader keeps
    // reporting itself as leader until its own leader lease expires, so its
    // stale self-report must be excluded: `await_leader` returns whichever
    // node answers first, and when the isolated leader is `nodes[0]` that
    // self-report would satisfy the poll meanwhile. Still tolerate windows
    // with no leader at all.
    let deadline = Instant::now() + Duration::from_secs(30);
    let _new_leader = loop {
        if let Ok(candidate) = harness.await_leader_excluding(first_leader).await {
            break candidate;
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "the majority must elect a leader other than the partitioned one: {}",
            harness.diagnostics()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert!(
        harness.max_term() > term_before,
        "electing a new leader must advance the term"
    );

    // Re-enable all proxies.
    toxi.reset()?;

    // The rejoining node must adopt the new leader's log rather than keep a
    // divergent one, so every node ends on the same applied index.
    harness.await_convergence(Duration::from_secs(30)).await?;

    harness.shutdown().await;
    Ok(())
}

/// A `limit_data` toxic truncates frames mid-stream. `read_frame` must return an
/// `UnexpectedEof` framing error and the connection loop must break without
/// panicking, leaving the cluster functional over the other links.
#[tokio::test]
async fn limit_data_truncated_frame_errors_not_panic() -> anyhow::Result<()> {
    let Some(harness) = common::RaftClusterHarness::try_start(3).await else {
        return Ok(());
    };
    let toxi = harness.toxiproxy();

    let leader_id = await_leader_within(&harness, Duration::from_secs(10)).await?;
    let leader_idx = (leader_id - 1) as usize;

    // 50 bytes truncates most frames on this follower link.
    let follower_idx = if leader_idx == 0 { 1 } else { 0 };
    let proxy = common::RaftClusterHarness::proxy_name(leader_idx, follower_idx);
    toxi.add_limit_data(&proxy, 50)?;

    // Wait 1 second for the toxic to disrupt some frames.
    tokio::time::sleep(Duration::from_secs(1)).await;

    toxi.reset()?;

    // Nothing panicked and the cluster is still live: it elects a leader and
    // every node agrees on one applied index once the toxic clears. A
    // re-election during the disruption is an acceptable outcome.
    //
    // The wait covers several leaderless election rounds: a truncated
    // replication can leave the faulted follower with an empty log, and its
    // higher-term campaigns keep stepping down the majority's elections.
    // Raft's up-to-date vote restriction blocks the empty-log node from
    // winning, so the cluster can stay leaderless until a full replication
    // round reaches it. Reaching it waits on the transport's 2s circuit-open
    // window, and behind that on openraft's backoff once enough consecutive
    // failures engage it, up to 12s at its cap.
    await_leader_within(&harness, Duration::from_secs(45)).await?;
    harness.await_convergence(Duration::from_secs(30)).await?;

    harness.shutdown().await;
    Ok(())
}
