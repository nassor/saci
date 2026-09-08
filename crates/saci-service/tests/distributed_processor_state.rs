// DistributedRunner threads a runtime's state blob across batches.
//
// Covers the host half of the `run-batch(input, prior) -> {.., checkpoint}`
// contract with a mock `PipelineRuntime` instead of a real WASM component: the
// mock can assert on exactly what `prior` it received, which a processor cannot
// report back. `tests/wasm_roundtrip.rs` covers the processor half against a real
// `.wasm`.
//
// The blob is written under the claim that produced it and read back under the
// claim that wrote the previous one. See `distributed::processor_state_store` for why
// the pointer has to be chained instead of derived from a stable partition key.

#![cfg(feature = "distributed")]

#[path = "common/memory_store.rs"]
mod memory_store;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use memory_store::MemoryStore;
use saci_core::runtime::PipelineRuntime;
use saci_core::{Dataset, SaciError, SaciResult};
use saci_service::distributed::checkpoint::{
    ACCUMULATOR_STAGE_SENTINEL, Checkpoint, CheckpointStore, PROCESSOR_STATE_STAGE_SENTINEL,
};
use saci_service::distributed::partition::{BatchClaim, PartitionSource};
use saci_service::distributed::runner::{DistributedRunner, RunnerConfig};
use saci_service::distributed::strategy::CheckpointStrategy;
use uuid::Uuid;

/// Shared log of every `prior` the runtime was handed, in call order.
type SeenPriors = Arc<Mutex<Vec<Option<Vec<u8>>>>>;

/// Decodes `prior` as a little-endian `u64`, increments it, and returns the new
/// value as the next blob. Records every `prior` it was handed so the test can
/// assert on the exact sequence.
struct MockCounterRuntime {
    seen_priors: SeenPriors,
    /// 1-based call index on which to fail instead of returning a blob.
    fail_on_call: usize,
    calls: Arc<AtomicUsize>,
}

impl MockCounterRuntime {
    fn new(fail_on_call: usize) -> (Self, SeenPriors) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let rt = Self {
            seen_priors: Arc::clone(&seen),
            fail_on_call,
            calls: Arc::new(AtomicUsize::new(0)),
        };
        (rt, seen)
    }

    fn decode(blob: Option<&[u8]>) -> u64 {
        match blob {
            Some(b) if b.len() == 8 => u64::from_le_bytes(b.try_into().expect("8 bytes")),
            _ => 0,
        }
    }
}

#[async_trait(?Send)]
impl PipelineRuntime for MockCounterRuntime {
    fn name(&self) -> &str {
        "mock-counter"
    }

    async fn run_on(&self, data: &mut Dataset) -> SaciResult<()> {
        self.run_on_with_state(data, None).await.map(|_| ())
    }

    async fn run_on_with_state(
        &self,
        _data: &mut Dataset,
        prior: Option<&[u8]>,
    ) -> SaciResult<Option<Vec<u8>>> {
        self.seen_priors
            .lock()
            .unwrap()
            .push(prior.map(<[u8]>::to_vec));

        let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if n == self.fail_on_call {
            return Err(SaciError::SystemExecution(
                "processor trap (run-batch): injected failure".to_string(),
            ));
        }

        let next = Self::decode(prior) + 1;
        Ok(Some(next.to_le_bytes().to_vec()))
    }

    fn template_dataset(&self) -> Dataset {
        Dataset::new()
    }
}

struct RecordingStoreInner {
    inner: MemoryStore,
    release_count: Arc<AtomicUsize>,
    ack_count: Arc<AtomicUsize>,
    claim_ids: Mutex<Vec<Uuid>>,
    /// Cap on claims issued: without it the runner re-claims a released batch
    /// forever, since release puts it straight back to Pending.
    max_claims: usize,
}

#[derive(Clone)]
struct RecordingStore(Arc<RecordingStoreInner>);

impl RecordingStore {
    fn new(inner: MemoryStore, max_claims: usize) -> (Self, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let release = Arc::new(AtomicUsize::new(0));
        let ack = Arc::new(AtomicUsize::new(0));
        let store = Self(Arc::new(RecordingStoreInner {
            inner,
            release_count: Arc::clone(&release),
            ack_count: Arc::clone(&ack),
            claim_ids: Mutex::new(Vec::new()),
            max_claims,
        }));
        (store, release, ack)
    }

    fn claim_ids(&self) -> Vec<Uuid> {
        self.0.claim_ids.lock().unwrap().clone()
    }
}

#[async_trait]
impl PartitionSource for RecordingStore {
    async fn claim_next_batch(&self, id: Uuid) -> SaciResult<Option<BatchClaim>> {
        if self.0.claim_ids.lock().unwrap().len() >= self.0.max_claims {
            return Ok(None);
        }
        let result = self.0.inner.claim_next_batch(id).await?;
        if let Some(claim) = result.as_ref() {
            self.0.claim_ids.lock().unwrap().push(claim.claim_id);
        }
        Ok(result)
    }

    async fn renew_claim(&self, claim_id: Uuid, instance_id: Uuid) -> SaciResult<u64> {
        self.0.inner.renew_claim(claim_id, instance_id).await
    }

    async fn ack_claim(&self, claim_id: Uuid, instance_id: Uuid) -> SaciResult<()> {
        self.0.ack_count.fetch_add(1, Ordering::SeqCst);
        self.0.inner.ack_claim(claim_id, instance_id).await
    }

    async fn release_claim(&self, claim_id: Uuid, instance_id: Uuid) -> SaciResult<()> {
        self.0.release_count.fetch_add(1, Ordering::SeqCst);
        self.0.inner.release_claim(claim_id, instance_id).await
    }

    fn should_renew(&self, _claim: &BatchClaim) -> bool {
        false
    }
}

#[async_trait]
impl CheckpointStore for RecordingStore {
    async fn save_checkpoint(
        &self,
        claim_id: Uuid,
        stage_idx: u32,
        ipc_bytes: Vec<u8>,
        schema_id: u32,
    ) -> SaciResult<()> {
        self.0
            .inner
            .save_checkpoint(claim_id, stage_idx, ipc_bytes, schema_id)
            .await
    }

    async fn load_checkpoint(
        &self,
        claim_id: Uuid,
        stage_idx: u32,
    ) -> SaciResult<Option<Checkpoint>> {
        self.0.inner.load_checkpoint(claim_id, stage_idx).await
    }
}

async fn seed_batch(store: &MemoryStore, batch_id: u64) {
    store
        .register_master_batch(batch_id, "test".to_string(), 1, vec![0u8; 16], 1)
        .await
        .expect("register_master_batch");
}

fn runner_config(max_batches: Option<usize>) -> RunnerConfig {
    RunnerConfig {
        max_batches,
        checkpoint_strategy: CheckpointStrategy::None,
        schema_id: 1,
        ..Default::default()
    }
}

fn counter_of(blob: &[u8]) -> u64 {
    u64::from_le_bytes(blob.try_into().expect("8-byte counter blob"))
}

/// Two successive batches: the second call must receive the first call's blob,
/// and the blob stored under the second claim must reflect both batches.
#[tokio::test]
async fn state_blob_carries_across_batches() {
    let inner = MemoryStore::new();
    seed_batch(&inner, 0).await;
    seed_batch(&inner, 1).await;

    let (store, release_count, ack_count) = RecordingStore::new(inner, 2);
    let (runtime, seen) = MockCounterRuntime::new(usize::MAX);

    let runner = DistributedRunner::new(store.clone(), Box::new(runtime), runner_config(Some(2)));
    let processed = runner.run(Dataset::new).await.expect("both batches run");
    assert_eq!(processed, 2);
    assert_eq!(release_count.load(Ordering::SeqCst), 0);
    assert_eq!(ack_count.load(Ordering::SeqCst), 2);

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2, "two batches → two runtime calls");
    assert!(seen[0].is_none(), "first batch must start cold");
    assert_eq!(
        seen[1].as_deref().map(counter_of),
        Some(1),
        "second batch must receive the first batch's blob"
    );

    // Each batch's blob is stored under the claim that produced it.
    let claims = store.claim_ids();
    assert_eq!(claims.len(), 2);
    for (i, claim_id) in claims.iter().enumerate() {
        let stored = store
            .load_checkpoint(*claim_id, PROCESSOR_STATE_STAGE_SENTINEL)
            .await
            .unwrap()
            .expect("state checkpoint present");
        assert_eq!(
            counter_of(&stored.payload),
            i as u64 + 1,
            "claim {i} must hold the counter after batch {}",
            i + 1
        );
    }
}

/// A fresh runner does not inherit the previous runner's state, because the
/// chained pointer lives in the loop, not on disk.
#[tokio::test]
async fn a_new_runner_starts_cold() {
    let inner = MemoryStore::new();
    seed_batch(&inner, 0).await;
    seed_batch(&inner, 1).await;

    let (store, _release, _ack) = RecordingStore::new(inner, 2);

    let (rt_a, seen_a) = MockCounterRuntime::new(usize::MAX);
    let runner_a = DistributedRunner::new(store.clone(), Box::new(rt_a), runner_config(Some(1)));
    runner_a.run(Dataset::new).await.expect("runner A batch");
    assert!(seen_a.lock().unwrap()[0].is_none());

    let (rt_b, seen_b) = MockCounterRuntime::new(usize::MAX);
    let runner_b = DistributedRunner::new(store.clone(), Box::new(rt_b), runner_config(Some(1)));
    runner_b.run(Dataset::new).await.expect("runner B batch");
    assert!(
        seen_b.lock().unwrap()[0].is_none(),
        "a fresh runner has no chained pointer and must start cold"
    );
}

/// A runtime error from `run_on_with_state` releases the claim rather than acking
/// it, and writes no state.
#[tokio::test]
async fn runtime_error_releases_claim_and_writes_no_state() {
    let inner = MemoryStore::new();
    seed_batch(&inner, 0).await;

    let (store, release_count, ack_count) = RecordingStore::new(inner, 1);
    let (runtime, _seen) = MockCounterRuntime::new(1);

    let runner = DistributedRunner::new(store.clone(), Box::new(runtime), runner_config(Some(1)));
    let err = runner
        .run(Dataset::new)
        .await
        .expect_err("failing runtime must surface an error");
    assert!(matches!(err, SaciError::SystemExecution(_)), "got: {err:?}");

    assert_eq!(release_count.load(Ordering::SeqCst), 1, "claim released");
    assert_eq!(ack_count.load(Ordering::SeqCst), 0, "claim not acked");

    let claim_id = store.claim_ids()[0];
    assert!(
        store
            .load_checkpoint(claim_id, PROCESSOR_STATE_STAGE_SENTINEL)
            .await
            .unwrap()
            .is_none(),
        "a failed batch must not persist state"
    );
}

/// The state sentinel does not shadow the accumulator sentinel.
#[tokio::test]
async fn state_and_accumulator_sentinels_are_independent() {
    let inner = MemoryStore::new();
    seed_batch(&inner, 0).await;

    let (store, _release, _ack) = RecordingStore::new(inner, 1);
    let (runtime, _seen) = MockCounterRuntime::new(usize::MAX);

    let runner = DistributedRunner::new(store.clone(), Box::new(runtime), runner_config(Some(1)));
    runner.run(Dataset::new).await.expect("batch runs");

    let claim_id = store.claim_ids()[0];

    // The claim is Completed by now, so seed the accumulator slot directly on
    // the same key to prove the two sentinels address different rows.
    store
        .save_checkpoint(claim_id, ACCUMULATOR_STAGE_SENTINEL, vec![7, 7], 1)
        .await
        .expect("write accumulator checkpoint");

    assert_eq!(
        store
            .load_checkpoint(claim_id, ACCUMULATOR_STAGE_SENTINEL)
            .await
            .unwrap()
            .expect("accumulator checkpoint present")
            .payload,
        vec![7, 7]
    );
    assert_eq!(
        counter_of(
            &store
                .load_checkpoint(claim_id, PROCESSOR_STATE_STAGE_SENTINEL)
                .await
                .unwrap()
                .expect("state checkpoint present")
                .payload
        ),
        1,
        "the state blob must be untouched by the accumulator write"
    );
}
