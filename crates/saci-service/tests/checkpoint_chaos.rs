//! Checkpoint atomicity and recovery. All of these run without Docker:
//!
//! - Truncated IPC bytes make `Dataset::read_ipc` fail instead of panicking.
//! - No vector in the committed conformance corpus makes `Dataset::read_ipc` panic,
//!   including the two whose buffer spans used to trip an arrow-rs assert.
//! - A failing `save_checkpoint` releases the claim instead of acking it.
//! - A truncated Parquet checkpoint file fails to load.

#![cfg(feature = "distributed")]

#[path = "common/memory_store.rs"]
mod memory_store;

use std::sync::Arc;

use memory_store::MemoryStore;
use saci_service::SaciError;
use saci_service::SaciResult;
use saci_service::dataset::Dataset;
use saci_service::distributed::checkpoint::{Checkpoint, CheckpointStore};
use saci_service::distributed::partition::{BatchClaim, PartitionSource};
use saci_service::distributed::runner::{DistributedRunner, RunnerConfig};
use saci_service::distributed::strategy::CheckpointStrategy;
use saci_service::pipeline::Pipeline;
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

/// Truncated IPC bytes make `Dataset::read_ipc` return `Err` rather than panic, so
/// the runner can surface a recoverable failure.
#[test]
fn torn_ipc_write_returns_error() {
    let dataset = Dataset::new();
    let mut ipc_bytes = Vec::new();
    dataset.write_ipc(&mut ipc_bytes).expect("write_ipc");
    assert!(!ipc_bytes.is_empty(), "IPC bytes must not be empty");

    let mut truncated = &ipc_bytes[..ipc_bytes.len() / 2];
    let result = Dataset::read_ipc(&mut truncated);
    assert!(
        result.is_err(),
        "truncated IPC must return Err, not Ok or panic"
    );
}

/// No vector in the committed cross-language conformance corpus
/// (`packages/arrow-ipc-conformance/`) may panic `Dataset::read_ipc`, whatever
/// `Result` it returns. Two vectors here — a buffer whose declared span
/// overruns its message body, and one reached via a negative length — used to
/// make arrow-rs assert instead of erroring; `read_ipc` now runs the decode
/// behind `catch_unwind`, so every caller inherits the fix, including a
/// corrupted checkpoint or window-accumulator blob `DistributedRunner` reads
/// back from storage.
#[test]
fn conformance_corpus_never_panics() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../packages/arrow-ipc-conformance/vectors");
    let mut checked = 0usize;
    for entry in std::fs::read_dir(&dir).expect("read conformance vectors dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().is_none_or(|e| e != "saci") {
            continue;
        }
        let bytes = std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        // Reaching the next iteration without unwinding is the assertion:
        // `read_ipc` must never panic, regardless of which `Result` it returns.
        let _ = Dataset::read_ipc(&mut &bytes[..]);
        checked += 1;
    }
    assert!(
        checked >= 15,
        "expected the full committed corpus, found only {checked} vector(s) in {}",
        dir.display()
    );
}

/// A failing `save_checkpoint` makes the runner release the claim instead of acking
/// it. Checked here through the full runner loop.
#[tokio::test]
async fn checkpoint_failure_integration_releases_not_acks() {
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let inner = MemoryStore::new();
    seed_batch(&inner, 0).await;

    let release_count = Arc::new(AtomicUsize::new(0));
    let ack_count = Arc::new(AtomicUsize::new(0));
    let claims_issued = Arc::new(AtomicUsize::new(0));

    struct FailSaveStore {
        inner: MemoryStore,
        release_count: Arc<AtomicUsize>,
        ack_count: Arc<AtomicUsize>,
        claims_issued: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl PartitionSource for FailSaveStore {
        async fn claim_next_batch(&self, id: Uuid) -> SaciResult<Option<BatchClaim>> {
            if self.claims_issued.load(Ordering::SeqCst) >= 1 {
                return Ok(None);
            }
            let r = self.inner.claim_next_batch(id).await?;
            if r.is_some() {
                self.claims_issued.fetch_add(1, Ordering::SeqCst);
            }
            Ok(r)
        }
        async fn renew_claim(&self, id: Uuid, instance_id: Uuid) -> SaciResult<u64> {
            self.inner.renew_claim(id, instance_id).await
        }
        async fn ack_claim(&self, id: Uuid, instance_id: Uuid) -> SaciResult<()> {
            self.ack_count.fetch_add(1, Ordering::SeqCst);
            self.inner.ack_claim(id, instance_id).await
        }
        async fn release_claim(&self, id: Uuid, instance_id: Uuid) -> SaciResult<()> {
            self.release_count.fetch_add(1, Ordering::SeqCst);
            self.inner.release_claim(id, instance_id).await
        }
    }

    #[async_trait]
    impl CheckpointStore for FailSaveStore {
        async fn save_checkpoint(&self, _: Uuid, _: u32, _: Vec<u8>, _: u32) -> SaciResult<()> {
            Err(SaciError::generic("simulated checkpoint failure"))
        }
        async fn load_checkpoint(&self, id: Uuid, stage: u32) -> SaciResult<Option<Checkpoint>> {
            self.inner.load_checkpoint(id, stage).await
        }
    }

    let store = FailSaveStore {
        inner,
        release_count: Arc::clone(&release_count),
        ack_count: Arc::clone(&ack_count),
        claims_issued: Arc::clone(&claims_issued),
    };

    let pipeline = Pipeline::new("test");
    let config = RunnerConfig {
        max_batches: Some(1),
        checkpoint_strategy: CheckpointStrategy::EveryStage,
        ..Default::default()
    };
    let runner = DistributedRunner::new(store, Box::new(pipeline), config);
    let processed = runner
        .run(empty_dataset)
        .await
        .expect("runner should not error");

    assert_eq!(processed, 0, "failed checkpoint → batch not counted");
    assert_eq!(
        release_count.load(Ordering::SeqCst),
        1,
        "must release on checkpoint failure"
    );
    assert_eq!(
        ack_count.load(Ordering::SeqCst),
        0,
        "must not ack on checkpoint failure"
    );
}

/// Parquet load rejects a checkpoint file truncated mid-write, before the atomic
/// rename landed.
#[cfg(feature = "parquet-checkpoint")]
#[tokio::test]
async fn parquet_load_rejects_crashed_tmp_file() {
    use saci_service::distributed::parquet_checkpoint::ParquetCheckpointStore;
    use tempfile::TempDir;

    let dir = TempDir::new().unwrap();
    let store = ParquetCheckpointStore::new(dir.path()).unwrap();
    let claim_id = Uuid::now_v7();

    // Write a valid checkpoint to get the final path.
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int32,
        false,
    )]));
    let batch = arrow_array::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    let mut ipc = Vec::new();
    {
        let mut w = arrow_ipc::writer::StreamWriter::try_new(&mut ipc, &schema).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
    }

    store.save_checkpoint(claim_id, 0, ipc, 1).await.unwrap();

    // Corrupt the final file (simulate a torn write that got renamed from .tmp).
    let pq_path = dir.path().join(format!("{claim_id}-stage0000.parquet"));
    let original = std::fs::read(&pq_path).unwrap();
    std::fs::write(&pq_path, &original[..original.len() / 2]).unwrap();

    let result = store.load_checkpoint(claim_id, 0).await;
    assert!(result.is_err(), "truncated Parquet must return Err");
}
