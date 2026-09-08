// A processor trap releases the claim instead of acking it.
//
// Uses a `MockTrapRuntime` instead of a real WASM component: the mock can be told
// to fail on an exact call index, which a real processor cannot do without carrying
// its own failure counter through `run-batch`'s state blob. The runner sees only
// a `SaciError`, so the release-not-ack path is identical either way.
//
// The mock keeps `PipelineRuntime`'s default `run_on_with_state`, so these tests
// also cover the stateless path: `prior` is ignored and no state blob is written.

#![cfg(feature = "distributed")]

#[path = "common/memory_store.rs"]
mod memory_store;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use memory_store::MemoryStore;
use saci_core::runtime::PipelineRuntime;
use saci_core::{Dataset, SaciError, SaciResult};
use saci_service::distributed::checkpoint::{Checkpoint, CheckpointStore};
use saci_service::distributed::partition::{BatchClaim, PartitionSource};
use saci_service::distributed::runner::{DistributedRunner, RunnerConfig};
use saci_service::distributed::strategy::CheckpointStrategy;
use uuid::Uuid;

/// Returns `SaciError::SystemExecution` on call `trap_on_call`, succeeds otherwise.
///
/// The error message mirrors the format produced by `WasmPipelineRuntime` so
/// assertions match the wording documented in the WIT spec:
/// `"processor trap (run-batch): ..."`.
struct MockTrapRuntime {
    call_count: Arc<AtomicUsize>,
    trap_on_call: usize,
}

impl MockTrapRuntime {
    fn new(trap_on_call: usize) -> (Self, Arc<AtomicUsize>) {
        let counter = Arc::new(AtomicUsize::new(0));
        let rt = Self {
            call_count: Arc::clone(&counter),
            trap_on_call,
        };
        (rt, counter)
    }
}

#[async_trait(?Send)]
impl PipelineRuntime for MockTrapRuntime {
    fn name(&self) -> &str {
        "mock-trap"
    }

    async fn run_on(&self, _data: &mut Dataset) -> SaciResult<()> {
        let n = self.call_count.fetch_add(1, Ordering::SeqCst) + 1;
        if n == self.trap_on_call {
            return Err(SaciError::SystemExecution(
                "processor trap (run-batch): unreachable executed".to_string(),
            ));
        }
        Ok(())
    }

    fn template_dataset(&self) -> Dataset {
        Dataset::new()
    }
}

/// Shared inner state for `CountingSource`.
struct CountingSourceInner {
    inner: MemoryStore,
    release_count: Arc<AtomicUsize>,
    ack_count: Arc<AtomicUsize>,
    claims_issued: Arc<AtomicUsize>,
    max_claims: usize,
}

/// Newtype over `Arc<CountingSourceInner>` so external traits can be implemented.
///
/// Counts `release_claim` and `ack_claim` calls and caps the total claims issued.
/// Without the cap the runner loops forever on a released batch: release returns
/// it to Pending, then `claim_next_batch` finds it again.
///
/// Cloneable, so both runners in the retry test share the counters and the store.
#[derive(Clone)]
struct CountingSource(Arc<CountingSourceInner>);

impl CountingSource {
    fn new(inner: MemoryStore, max_claims: usize) -> (Self, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let release = Arc::new(AtomicUsize::new(0));
        let ack = Arc::new(AtomicUsize::new(0));
        let src = Self(Arc::new(CountingSourceInner {
            inner,
            release_count: Arc::clone(&release),
            ack_count: Arc::clone(&ack),
            claims_issued: Arc::new(AtomicUsize::new(0)),
            max_claims,
        }));
        (src, release, ack)
    }
}

#[async_trait]
impl PartitionSource for CountingSource {
    async fn claim_next_batch(&self, id: Uuid) -> SaciResult<Option<BatchClaim>> {
        if self.0.claims_issued.load(Ordering::SeqCst) >= self.0.max_claims {
            return Ok(None);
        }
        let result = self.0.inner.claim_next_batch(id).await?;
        if result.is_some() {
            self.0.claims_issued.fetch_add(1, Ordering::SeqCst);
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
impl CheckpointStore for CountingSource {
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

/// Trap on the first (and only) batch: runner must return `Err`, call
/// `release_claim` exactly once, and never call `ack_claim`.
#[tokio::test]
async fn test_trap_releases_not_acks() {
    let inner = MemoryStore::new();
    seed_batch(&inner, 0).await;

    let (source, release_count, ack_count) = CountingSource::new(inner, 1);
    let (runtime, _call_count) = MockTrapRuntime::new(1);

    let runner = DistributedRunner::new(source, Box::new(runtime), runner_config(Some(1)));
    let result = runner.run(Dataset::new).await;

    let err = result.expect_err("expected Err from trapping runtime");
    let msg = err.to_string();
    assert!(
        msg.contains("processor trap (run-batch)"),
        "error message must reference processor trap; got: {msg}"
    );
    assert_eq!(
        release_count.load(Ordering::SeqCst),
        1,
        "claim must be released exactly once on runtime error"
    );
    assert_eq!(
        ack_count.load(Ordering::SeqCst),
        0,
        "claim must NOT be acked when runtime returns Err"
    );
}

/// A trapped batch is released, re-claimable, and succeeds on the retry.
///
/// One `CountingSource` is shared by both `DistributedRunner` instances so they
/// see the same `release_count` and `ack_count`. The first runner exits with
/// `Err`; the second claims the re-queued batch. After both: release 1, ack 1.
#[tokio::test]
async fn test_trap_then_retry_succeeds() {
    let inner = MemoryStore::new();
    seed_batch(&inner, 0).await;

    // Allow 2 total claims across both runner runs (1 trap + 1 retry).
    let (source, release_count, ack_count) = CountingSource::new(inner, 2);

    let (trap_rt, call_count) = MockTrapRuntime::new(1);
    let runner1 = DistributedRunner::new(source.clone(), Box::new(trap_rt), runner_config(Some(1)));
    let result1 = runner1.run(Dataset::new).await;
    assert!(result1.is_err(), "run 1 must fail on trap");
    assert_eq!(release_count.load(Ordering::SeqCst), 1, "run 1: release==1");
    assert_eq!(ack_count.load(Ordering::SeqCst), 0, "run 1: ack==0");
    assert_eq!(call_count.load(Ordering::SeqCst), 1, "runtime called once");

    let (ok_rt, _) = MockTrapRuntime::new(usize::MAX); // never traps
    let runner2 = DistributedRunner::new(source.clone(), Box::new(ok_rt), runner_config(Some(1)));
    let processed = runner2.run(Dataset::new).await.expect("run 2 must succeed");

    assert_eq!(processed, 1, "retry must process the re-queued batch");
    assert_eq!(
        release_count.load(Ordering::SeqCst),
        1,
        "run 2 must not add more releases"
    );
    assert_eq!(ack_count.load(Ordering::SeqCst), 1, "run 2: batch acked");
}

/// The trap surfaces as `SaciError::SystemExecution`, not a panic and not a
/// re-wrapped error.
#[tokio::test]
async fn test_trap_error_is_system_execution_variant() {
    let inner = MemoryStore::new();
    seed_batch(&inner, 0).await;

    let (source, _release, _ack) = CountingSource::new(inner, 1);
    let (runtime, _) = MockTrapRuntime::new(1);

    let runner = DistributedRunner::new(source, Box::new(runtime), runner_config(Some(1)));
    let err = runner.run(Dataset::new).await.unwrap_err();

    assert!(
        matches!(err, SaciError::SystemExecution(_)),
        "error must be SaciError::SystemExecution, got: {err:?}"
    );
}
