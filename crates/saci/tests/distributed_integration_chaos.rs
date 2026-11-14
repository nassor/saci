//! Full-stack chaos test for the Raft consensus layer under combined fault
//! injection. Requires Docker and Toxiproxy, and runs for about 70 seconds.
//!
//! ```bash
//! cargo test --features distributed-raft \
//!     --test distributed_integration_chaos -- --ignored --nocapture
//! ```
//!
//! - 5-node Raft cluster with all TCP edges proxied through Toxiproxy.
//! - 60-second chaos window: random latency, bandwidth, reset-peer and full
//!   partition faults on random edges, interrupted at the midpoint by one
//!   deterministic isolation of the current leader. The random schedule alone
//!   only unseats a leader by luck (see the comment on the isolation below), so
//!   the election that proves liveness under faults is forced, not hoped for.
//! - The forced isolation asserts the surviving quorum elects a different
//!   leader on a higher term.
//! - After the chaos window + 10s settle:
//!   - The applied index converges to the same value on all 5 nodes, so no node
//!     kept a divergent log.
//!   - The cluster still elects a leader after healing.

#[cfg(feature = "distributed-raft")]
mod common;

/// Inject random single-edge faults until `until`, ignoring Toxiproxy errors:
/// a toxic that fails to apply is one less fault, not a test failure.
#[cfg(feature = "distributed-raft")]
async fn random_chaos(toxi: &common::ToxiproxyClient, n_nodes: usize, until: tokio::time::Instant) {
    use common::RaftClusterHarness;
    use rand::RngExt;
    use std::time::Duration;

    let mut rng = rand::rng();

    while tokio::time::Instant::now() < until {
        let src = rng.random_range(0..n_nodes);
        let dst = loop {
            let d = rng.random_range(0..n_nodes);
            if d != src {
                break d;
            }
        };
        let proxy_name = RaftClusterHarness::proxy_name(src, dst);

        let action = rng.random_range(0u32..4);
        match action {
            // Latency: add, hold briefly, remove.
            0 => {
                let ms = rng.random_range(0u64..=500);
                let hold_ms = rng.random_range(1000u64..=3000);
                let _ = toxi.add_latency(&proxy_name, ms);
                tokio::time::sleep(Duration::from_millis(hold_ms)).await;
                let _ = toxi.delete_toxic(&proxy_name, "latency_upstream");
            }
            // Bandwidth: add, hold briefly, remove.
            1 => {
                // 10 KB/s = 80 kbps to 10 MB/s = 80_000 kbps
                let kbps = rng.random_range(80u64..=80_000);
                let hold_ms = rng.random_range(1000u64..=3000);
                let _ = toxi.add_bandwidth(&proxy_name, kbps);
                tokio::time::sleep(Duration::from_millis(hold_ms)).await;
                let _ = toxi.delete_toxic(&proxy_name, "bandwidth_upstream");
            }
            // Reset peer: short hold.
            2 => {
                let timeout_ms = rng.random_range(0u64..=200);
                let hold_ms = rng.random_range(200u64..=500);
                let _ = toxi.add_reset_peer(&proxy_name, timeout_ms);
                tokio::time::sleep(Duration::from_millis(hold_ms)).await;
                let _ = toxi.delete_toxic(&proxy_name, "reset_peer");
            }
            // Full partition: disable then re-enable.
            _ => {
                let hold_ms = rng.random_range(500u64..=5000);
                let _ = toxi.disable_proxy(&proxy_name);
                tokio::time::sleep(Duration::from_millis(hold_ms)).await;
                let _ = toxi.enable_proxy(&proxy_name);
            }
        }

        let sleep_ms = rng.random_range(200u64..=2000);
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    }
}

#[cfg(feature = "distributed-raft")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spins up a 5-node Raft cluster behind Toxiproxy and chaos-injects faults for ~70-100s"]
async fn full_stack_chaos_monkey_60s() {
    use std::time::Duration;

    use common::RaftClusterHarness;

    let Some(harness) = RaftClusterHarness::try_start(5).await else {
        return;
    };

    harness
        .await_leader()
        .await
        .expect("leader should be elected before chaos");

    const N_NODES: usize = 5;
    const CHAOS_DURATION: Duration = Duration::from_secs(60);

    let toxi = harness.toxiproxy();
    let chaos_end = tokio::time::Instant::now() + CHAOS_DURATION;

    random_chaos(toxi, N_NODES, chaos_end - CHAOS_DURATION / 2).await;

    // A random single-edge fault only starves a follower of heartbeats when it
    // lands on an edge *out of* the leader: 4 of the 20 directed edges, and
    // then only for a full partition or a latency draw above the 300-500 ms
    // election timeout. A 60 s window runs ~19 iterations, so the random
    // schedule alone leaves the term untouched about one run in ten, which is
    // what "Raft term must have advanced" failed on. Cutting every link of the
    // current leader forces the election instead, so the liveness claim below
    // is a statement about Raft rather than about the dice.
    //
    // Read the term before reading the leader: if the random half unseats the
    // leader between the two reads, `unseated` is the *new* leader and
    // `term_before_isolation` still predates it, so isolating `unseated`
    // forces one more election either way.
    let term_before_isolation = harness.max_term();
    let unseated = harness
        .await_leader()
        .await
        .expect("chaos must leave the cluster with a leader to isolate");
    harness
        .isolate_node((unseated - 1) as usize)
        .expect("Toxiproxy must accept the leader isolation");

    // The isolated node keeps reporting itself as leader until its own leader
    // lease expires, so its stale self-report is excluded. The other four hold
    // quorum, so the new election costs one election timeout; 30 s of slack
    // covers a runner slow enough to lose several rounds to split votes.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let new_leader = loop {
        if let Ok(candidate) = harness.await_leader_excluding(unseated).await {
            break candidate;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "isolating leader {unseated} must force a new election: {}",
            harness.diagnostics()
        );
    };
    assert!(
        harness.max_term() > term_before_isolation,
        "electing {new_leader} over the isolated leader {unseated} must advance \
         the term past {term_before_isolation}: {}",
        harness.diagnostics()
    );

    harness
        .rejoin_node((unseated - 1) as usize)
        .expect("Toxiproxy must restore the isolated leader's links");

    // The rest of the window: random faults again, now around a leader that
    // took office mid-chaos and an ex-leader that has to catch up.
    random_chaos(toxi, N_NODES, chaos_end).await;

    // After the chaos window: reset all proxies and let the cluster settle.
    let _ = toxi.reset();
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Replication is deterministic, so every node must land on the same applied
    // index. A node that kept a divergent log would never match.
    harness
        .await_convergence(Duration::from_secs(60))
        .await
        .expect("all 5 nodes must converge to the same applied index");

    // The healed cluster is still electable.
    harness
        .await_leader()
        .await
        .expect("a healed cluster must still hold a leader");

    harness.shutdown().await;
}
