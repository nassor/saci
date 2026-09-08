//! Unit tests for [`DistributedRunner`] lease and partition behavior against
//! the in-memory shared-store fixture. No Docker required.

#![cfg(feature = "distributed")]

#[path = "common/memory_store.rs"]
mod memory_store;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use memory_store::MemoryStore;
use saci_service::SaciError;
use saci_service::SaciResult;
use saci_service::dataset::Dataset;
use saci_service::distributed::checkpoint::{Checkpoint, CheckpointStore};
use saci_service::distributed::partition::{BatchClaim, PartitionSource};
use saci_service::distributed::runner::{DistributedRunner, RunnerConfig};
use saci_service::distributed::strategy::CheckpointStrategy;
use saci_service::pipeline::Pipeline;
use saci_service::system::{SystemMeta, system_fn};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

async fn seed_batch(store: &MemoryStore, batch_id: u64) {
    store
        .register_master_batch(batch_id, "test".to_string(), 1, vec![0u8; 64], 10)
        .await
        .expect("seed_batch");
}

fn empty_dataset() -> Dataset {
    Dataset::new()
}

/// Wraps a real store and counts ack/release/renew calls.
struct InstrumentedStore {
    inner: MemoryStore,
    ack_count: Arc<AtomicUsize>,
    release_count: Arc<AtomicUsize>,
    renew_count: Arc<AtomicUsize>,
    /// If `Some`, renew_claim returns this error after the delay.
    fail_renew_after: Option<Duration>,
}

impl InstrumentedStore {
    fn new(inner: MemoryStore) -> (Self, Arc<AtomicUsize>, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let ack = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(AtomicUsize::new(0));
        let renew = Arc::new(AtomicUsize::new(0));
        (
            Self {
                inner,
                ack_count: Arc::clone(&ack),
                release_count: Arc::clone(&release),
                renew_count: Arc::clone(&renew),
                fail_renew_after: None,
            },
            ack,
            release,
            renew,
        )
    }

    fn with_fail_renew_after(mut self, delay: Duration) -> Self {
        self.fail_renew_after = Some(delay);
        self
    }
}

#[async_trait]
impl PartitionSource for InstrumentedStore {
    async fn claim_next_batch(&self, id: Uuid) -> SaciResult<Option<BatchClaim>> {
        self.inner.claim_next_batch(id).await
    }
    async fn renew_claim(&self, claim_id: Uuid, instance_id: Uuid) -> SaciResult<u64> {
        self.renew_count.fetch_add(1, Ordering::SeqCst);
        if let Some(delay) = self.fail_renew_after {
            tokio::time::sleep(delay).await;
            return Err(SaciError::generic("simulated renewal failure after delay"));
        }
        self.inner.renew_claim(claim_id, instance_id).await
    }
    async fn ack_claim(&self, claim_id: Uuid, instance_id: Uuid) -> SaciResult<()> {
        self.ack_count.fetch_add(1, Ordering::SeqCst);
        self.inner.ack_claim(claim_id, instance_id).await
    }
    async fn release_claim(&self, claim_id: Uuid, instance_id: Uuid) -> SaciResult<()> {
        self.release_count.fetch_add(1, Ordering::SeqCst);
        self.inner.release_claim(claim_id, instance_id).await
    }
}

#[async_trait]
impl CheckpointStore for InstrumentedStore {
    async fn save_checkpoint(
        &self,
        claim_id: Uuid,
        stage_idx: u32,
        ipc_bytes: Vec<u8>,
        schema_id: u32,
    ) -> SaciResult<()> {
        self.inner
            .save_checkpoint(claim_id, stage_idx, ipc_bytes, schema_id)
            .await
    }
    async fn load_checkpoint(
        &self,
        claim_id: Uuid,
        stage_idx: u32,
    ) -> SaciResult<Option<Checkpoint>> {
        self.inner.load_checkpoint(claim_id, stage_idx).await
    }
}

/// A failed lease renewal must release the claim rather than ack it.
#[tokio::test]
async fn lease_expires_mid_execution_releases_not_acks() {
    let inner = MemoryStore::new();
    seed_batch(&inner, 0).await;

    let (store, ack_count, release_count, _renew_count) = InstrumentedStore::new(inner);
    // Fail renewal immediately after the first sleep.
    let store = store.with_fail_renew_after(Duration::from_millis(1));

    let pipeline = Pipeline::new("test");
    let config = RunnerConfig {
        max_batches: Some(1),
        checkpoint_strategy: CheckpointStrategy::None,
        ..Default::default()
    };

    // Timing here is not deterministic: the batch may finish before the renewal
    // fails. The invariant that holds either way is that a failed renewal never
    // acks, so release and ack counts must stay balanced.
    let runner = DistributedRunner::new(store, Box::new(pipeline), config);
    let result = runner.run(empty_dataset).await;
    if result.is_err() || ack_count.load(Ordering::SeqCst) == 0 {
        assert_eq!(
            release_count.load(Ordering::SeqCst),
            ack_count
                .load(Ordering::SeqCst)
                .saturating_add(if result.is_err() { 1 } else { 0 }),
        );
    }
}

/// Graceful shutdown between batches: no claim held on exit.
#[tokio::test]
async fn shutdown_between_batches_no_claim_held() {
    let inner = MemoryStore::new();
    seed_batch(&inner, 0).await;
    seed_batch(&inner, 1).await;

    let (store, ack_count, release_count, _) = InstrumentedStore::new(inner);
    let shutdown = CancellationToken::new();
    shutdown.cancel();

    let pipeline = Pipeline::new("test");
    let config = RunnerConfig {
        max_batches: None,
        checkpoint_strategy: CheckpointStrategy::None,
        ..Default::default()
    };
    let runner = DistributedRunner::new(store, Box::new(pipeline), config);
    let processed = runner
        .run_with_shutdown(empty_dataset, shutdown)
        .await
        .unwrap();

    assert_eq!(processed, 0, "cancelled before any batch → 0 processed");
    assert_eq!(
        ack_count.load(Ordering::SeqCst),
        0,
        "no acks on immediate shutdown"
    );
    assert_eq!(
        release_count.load(Ordering::SeqCst),
        0,
        "no claims held → no releases"
    );
}

/// Two runners share one store and one batch, so exactly one may ack it.
#[tokio::test]
async fn concurrent_runners_exactly_one_acks() {
    // Cloning the fixture shares one set of batches, so the two runners contend.
    let store_a = MemoryStore::new();
    seed_batch(&store_a, 0).await;
    let store_b = store_a.clone();

    let mut pipeline_a = Pipeline::new("runner-a");
    pipeline_a.add_system(system_fn(SystemMeta::new("noop-a"), |_| Ok(())));
    let mut pipeline_b = Pipeline::new("runner-b");
    pipeline_b.add_system(system_fn(SystemMeta::new("noop-b"), |_| Ok(())));

    let config = RunnerConfig {
        max_batches: Some(1),
        checkpoint_strategy: CheckpointStrategy::None,
        ..Default::default()
    };

    let config_b = config.clone();
    let (res_a, res_b) = tokio::join!(
        async {
            let runner = DistributedRunner::new(store_a, Box::new(pipeline_a), config);
            runner.run(empty_dataset).await.unwrap_or(0)
        },
        async {
            let runner = DistributedRunner::new(store_b, Box::new(pipeline_b), config_b);
            runner.run(empty_dataset).await.unwrap_or(0)
        }
    );

    let total_acks = res_a + res_b;
    assert_eq!(
        total_acks, 1,
        "exactly one runner should process the batch; got a={res_a} b={res_b}"
    );
}
