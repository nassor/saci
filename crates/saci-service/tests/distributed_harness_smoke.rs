//! Smoke test for the multi-node Raft chaos harness.
//!
//! Soft-skips when Docker is unavailable. Run explicitly with:
//!
//! ```text
//! cargo test --features distributed-raft --test distributed_harness_smoke
//! ```

#[cfg(feature = "distributed-raft")]
mod common;

/// A three-node cluster elects a leader, commits the leader's own term entry,
/// and every node converges on the same applied index. Nothing is proposed:
/// the SACI raft carries membership and leadership only.
#[cfg(feature = "distributed-raft")]
#[tokio::test(flavor = "multi_thread")]
async fn cluster_forms_and_converges() {
    use common::RaftClusterHarness;
    use std::time::Duration;

    let Some(harness) = RaftClusterHarness::try_start(3).await else {
        return;
    };

    let leader: u64 = harness
        .await_leader()
        .await
        .expect("leader should be elected within 10 s");

    // Taking office appends one entry, so the leader's applied index must pass 0.
    let leader_applied = harness
        .await_applied_at_least(leader, 1, Duration::from_secs(10))
        .await
        .expect("the leader must apply its own term entry");

    let converged = harness
        .await_convergence(Duration::from_secs(15))
        .await
        .expect("all three nodes must converge on one applied index");
    assert!(
        converged >= leader_applied,
        "convergence index {converged} must not go backwards from {leader_applied}"
    );

    assert!(
        harness.max_term() >= 1,
        "an elected leader implies a term of at least 1"
    );

    harness.shutdown().await;
}
