//! Integration tests for the redb-backed shared store and state client.
//!
//! Both stores are embedded files, so nothing here needs Docker: the suite
//! runs in the fast nextest profile. Cluster behaviour (proposals through a
//! live raft) is covered by the chaos suites.

#![cfg(feature = "distributed-raft")]

use std::time::Duration;

use saci_service::SaciResult;
use saci_service::distributed::checkpoint::CheckpointStore;
use saci_service::distributed::consensus::store::RedbSharedStore;
use saci_service::distributed::partition::{MAX_LOG_ENTRY_BYTES, PartitionSource};
use saci_service::service::redb_state::{RedbStateClient, SourceCursorMeta};
use tempfile::TempDir;
use uuid::Uuid;

/// Rows per registered master batch. A claim covers the whole uncovered gap of
/// one batch, so "the next range" means the next batch's rows.
const BATCH_ROWS: u32 = 512;

fn store(dir: &TempDir) -> RedbSharedStore {
    RedbSharedStore::single_node(&dir.path().join("app.redb")).expect("open store")
}

async fn register(store: &RedbSharedStore, batch_id: u64) -> SaciResult<()> {
    store
        .register_master_batch(batch_id, "orders".to_string(), 1, vec![0u8; 64], BATCH_ROWS)
        .await
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_millis() as u64
}

#[tokio::test]
async fn store_claim_checkpoint_ack_flow() {
    let dir = TempDir::new().expect("tempdir");
    let store = store(&dir);
    register(&store, 1).await.expect("register 1");
    register(&store, 2).await.expect("register 2");

    let instance = Uuid::now_v7();

    // First claim: batch 1's whole row range, with the documented lease math.
    let claim = store
        .claim_next_batch(instance)
        .await
        .expect("claim")
        .expect("one pending range");
    assert_eq!(claim.batch_id, 1);
    assert_eq!(claim.component, "orders");
    assert_eq!(claim.row_range, 0..BATCH_ROWS);
    assert_eq!(claim.schema_id, 1);
    assert_eq!(claim.instance_id, instance);
    assert_eq!(claim.lease_ttl_millis, 90_000);
    assert!(
        claim.lease_expires_at > 0,
        "lease expiry must be stamped in the future"
    );

    // Checkpoint round-trip; the schema-id ledger updates on data stages.
    assert_eq!(store.persisted_schema_id().await.expect("schema id"), None);
    let payload = vec![0xAB; 128];
    store
        .save_checkpoint(claim.claim_id, 3, payload.clone(), 7)
        .await
        .expect("save checkpoint");
    let cp = store
        .load_checkpoint(claim.claim_id, 3)
        .await
        .expect("load checkpoint")
        .expect("checkpoint exists");
    assert_eq!(cp.payload, payload);
    assert_eq!(cp.schema_id, 7);
    assert_eq!(
        store.persisted_schema_id().await.expect("schema id"),
        Some(7)
    );

    // Renewal extends the expiry.
    let renewed = store
        .renew_claim(claim.claim_id, instance)
        .await
        .expect("renew");
    assert!(
        renewed >= claim.lease_expires_at,
        "renewal must not move the lease backward"
    );

    // Ack; the next claim moves on to the second batch.
    store
        .ack_claim(claim.claim_id, instance)
        .await
        .expect("ack");
    let claim2 = store
        .claim_next_batch(instance)
        .await
        .expect("claim 2")
        .expect("second pending range");
    assert_eq!(claim2.batch_id, 2);
    assert_eq!(claim2.row_range, 0..BATCH_ROWS);
    assert_ne!(claim2.claim_id, claim.claim_id);
    store
        .ack_claim(claim2.claim_id, instance)
        .await
        .expect("ack 2");

    // Everything acked: nothing left to claim.
    assert!(
        store
            .claim_next_batch(instance)
            .await
            .expect("claim")
            .is_none()
    );
}

#[tokio::test]
async fn restart_resumes_from_completed_ranges() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("app.redb");

    let instance = Uuid::now_v7();
    {
        let store = RedbSharedStore::single_node(&db_path).expect("open store");
        register(&store, 1).await.expect("register 1");
        register(&store, 2).await.expect("register 2");
        let claim = store
            .claim_next_batch(instance)
            .await
            .expect("claim")
            .expect("batch 1");
        assert_eq!(claim.batch_id, 1);
        store
            .ack_claim(claim.claim_id, instance)
            .await
            .expect("ack batch 1");
    }

    // A "restart": a fresh store over the same redb file.
    let store = RedbSharedStore::single_node(&db_path).expect("reopen store");
    let resumed = store
        .claim_next_batch(instance)
        .await
        .expect("claim after restart")
        .expect("batch 2 still pending");
    assert_eq!(
        resumed.batch_id, 2,
        "a restarted service must never re-claim a completed range"
    );
}

#[tokio::test]
async fn concurrent_claims_are_exclusive() {
    let dir = TempDir::new().expect("tempdir");
    let store = std::sync::Arc::new(store(&dir));
    register(&store, 1).await.expect("register");

    let a = {
        let store = std::sync::Arc::clone(&store);
        tokio::spawn(async move { store.claim_next_batch(Uuid::now_v7()).await })
    };
    let b = {
        let store = std::sync::Arc::clone(&store);
        tokio::spawn(async move { store.claim_next_batch(Uuid::now_v7()).await })
    };
    let a = a.await.expect("task a").expect("a claim");
    let b = b.await.expect("task b").expect("b claim");
    match (a, b) {
        (Some(_), None) | (None, Some(_)) => {}
        (Some(a), Some(b)) => panic!("both callers claimed the same range: {a:?} vs {b:?}"),
        (None, None) => panic!("neither caller claimed the pending range"),
    }
}

#[tokio::test]
async fn reclaim_expired_frees_claim() {
    let dir = TempDir::new().expect("tempdir");
    let store = store(&dir).with_lease_ttl_millis(1_000);
    register(&store, 1).await.expect("register");

    let claim = store
        .claim_next_batch(Uuid::now_v7())
        .await
        .expect("claim")
        .expect("pending range");

    // Lease is 1 s; two seconds on it has expired and must be reclaimed.
    assert_eq!(
        store
            .reclaim_expired(now_millis() + 2_000)
            .await
            .expect("reclaim"),
        1,
        "the expired claim must be freed"
    );
    let _ = claim;
    let re_claimed = store
        .claim_next_batch(Uuid::now_v7())
        .await
        .expect("claim after reclaim")
        .expect("range claimable again");
    assert_eq!(re_claimed.row_range, 0..BATCH_ROWS);
}

#[tokio::test]
async fn renew_after_expiry_fails() {
    let dir = TempDir::new().expect("tempdir");
    let store = store(&dir).with_lease_ttl_millis(1_000);
    register(&store, 1).await.expect("register");

    let instance = Uuid::now_v7();
    let claim = store
        .claim_next_batch(instance)
        .await
        .expect("claim")
        .expect("pending range");

    // Wait out the 1 s lease (plus a margin), then renew: the lease contract
    // says a lost claim must surface as an error, not a silent success.
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    let err = store
        .renew_claim(claim.claim_id, instance)
        .await
        .expect_err("renewing an expired claim must fail");
    assert!(
        err.to_string().contains("claim") || err.to_string().contains("lease"),
        "error should name the claim/lease: {err}"
    );
}

/// A checkpoint travels inside a raft log entry, so the store's envelope is
/// [`MAX_LOG_ENTRY_BYTES`] and an oversize payload is refused at the propose
/// boundary rather than truncated or split silently.
#[tokio::test]
async fn checkpoint_over_cap_is_rejected() {
    let dir = TempDir::new().expect("tempdir");
    let store = store(&dir);
    register(&store, 1).await.expect("register");
    let claim = store
        .claim_next_batch(Uuid::now_v7())
        .await
        .expect("claim")
        .expect("pending range");

    assert_eq!(
        store.max_checkpoint_bytes(),
        MAX_LOG_ENTRY_BYTES,
        "the store must not advertise an envelope larger than a log entry"
    );

    let err = store
        .save_checkpoint(claim.claim_id, 0, vec![0u8; MAX_LOG_ENTRY_BYTES + 1], 1)
        .await
        .expect_err("an oversize checkpoint must be rejected");
    assert!(
        err.to_string().contains("MAX_LOG_ENTRY_BYTES"),
        "error should name the cap: {err}"
    );
}

/// The standalone/stream persistence path: config bytes, processor priors and
/// source cursors all survive a reopen of the same file.
#[tokio::test]
async fn config_and_cursor_roundtrip() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("state.redb");
    let client = RedbStateClient::open(&path).expect("open");
    let workflow = "wf-roundtrip";
    let node = "proc-1";
    let source = "src-1";

    let config_bytes = b"mode \"standalone\"\nnode id=1 data_dir=\"/tmp/saci\"\n";
    client
        .put_config("saci.kdl", config_bytes)
        .await
        .expect("put config");

    // Prior save/load/delete.
    assert_eq!(
        client.load_prior(workflow, node).await.expect("load prior"),
        None,
        "no prior before the first save"
    );
    client
        .save_prior(workflow, node, b"state-blob")
        .await
        .expect("save prior");
    assert_eq!(
        client
            .load_prior(workflow, node)
            .await
            .expect("load prior")
            .as_deref(),
        Some(&b"state-blob"[..])
    );
    client
        .delete_prior(workflow, node)
        .await
        .expect("delete prior");
    assert_eq!(
        client
            .load_prior(workflow, node)
            .await
            .expect("load prior after delete"),
        None
    );

    // Cursor save/load.
    let meta = SourceCursorMeta {
        items_processed: 42,
        last_batch_at_ms: 1_700_000_000_000,
    };
    assert_eq!(
        client
            .load_source_cursor(workflow, source)
            .await
            .expect("load cursor"),
        None
    );
    client
        .save_source_cursor(workflow, source, meta)
        .await
        .expect("save cursor");
    client
        .save_prior(workflow, node, b"final-blob")
        .await
        .expect("save prior again");

    // Reopen the same file: everything committed must still be there.
    drop(client);
    let client = RedbStateClient::open(&path).expect("reopen");
    assert_eq!(
        client
            .load_source_cursor(workflow, source)
            .await
            .expect("load cursor after reopen"),
        Some(meta)
    );
    assert_eq!(
        client
            .load_prior(workflow, node)
            .await
            .expect("load prior after reopen")
            .as_deref(),
        Some(&b"final-blob"[..])
    );
}

/// The accumulator chain-carry over a redb-backed runner: claim 2 must start
/// from claim 1's accumulator rows, and the runner processes both claims.
#[cfg(feature = "windows")]
#[tokio::test]
async fn runner_window_accumulator_carries_across_claims() {
    use std::sync::Arc as StdArc;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicU32, Ordering};

    use saci_core::SystemMeta;
    use saci_core::component::Component as _;
    use saci_core::dataset::Dataset;
    use saci_core::pipeline::Pipeline;
    use saci_core::system::system_fn;
    use saci_core::windows::accumulator::WindowAccumulator;
    use saci_service::distributed::runner::{DistributedRunner, RunnerConfig};
    use saci_service::distributed::strategy::CheckpointStrategy;

    let dir = TempDir::new().expect("tempdir");
    let store = RedbSharedStore::single_node(&dir.path().join("app.redb")).expect("open store");

    // Two batches → two claims; only the chained accumulator carries rows
    // from the first claim to the second.
    for batch_id in 0u64..2 {
        store
            .register_master_batch(batch_id, "test".to_string(), 1, vec![0u8; 64], 10)
            .await
            .expect("register");
    }

    let run_count = StdArc::new(AtomicU32::new(0));
    let entry_rows = StdArc::new(StdMutex::new(Vec::<u64>::new()));
    let run_count_clone = StdArc::clone(&run_count);
    let entry_rows_clone = StdArc::clone(&entry_rows);

    let mut pipeline = Pipeline::new("test");
    pipeline.add_system(system_fn(
        SystemMeta::new("append_accumulator"),
        move |data: &mut Dataset| {
            entry_rows_clone
                .lock()
                .expect("entry rows mutex poisoned")
                .push(data.rows() as u64);
            let run = run_count_clone.fetch_add(1, Ordering::Relaxed);
            if data.batch_for(WindowAccumulator::name()).is_some() {
                let row = WindowAccumulator {
                    version: Some(1),
                    source_component: "test".to_string(),
                    window_id: run as i64,
                    key_hash: 0,
                    count: 1,
                    sum_f64: Some(run as f64 + 1.0),
                    min_f64: None,
                    max_f64: None,
                    session_start_ts: None,
                    session_end_ts: None,
                    finalized_at_watermark: None,
                };
                data.append::<WindowAccumulator>(&[row])
                    .expect("append accumulator row");
            }
            Ok(())
        },
    ));

    let world_factory = || {
        let mut d = Dataset::new();
        d.register_component::<WindowAccumulator>()
            .expect("register accumulator component");
        d
    };

    let config = RunnerConfig {
        max_batches: Some(2),
        checkpoint_strategy: CheckpointStrategy::None,
        ..Default::default()
    };
    let runner = DistributedRunner::new(store, Box::new(pipeline), config);
    let processed = runner.run(world_factory).await.expect("run");
    assert_eq!(processed, 2);
    assert_eq!(run_count.load(Ordering::Relaxed), 2);
    assert_eq!(
        entry_rows
            .lock()
            .expect("entry rows mutex poisoned")
            .as_slice(),
        &[0, 1],
        "the second claim must start from the first claim's accumulator rows"
    );
}
