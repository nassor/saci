//! Helper functions for persisting and restoring a runtime's opaque state blob.
//!
//! Both functions use [`PROCESSOR_STATE_STAGE_SENTINEL`] as the `stage_idx` so the
//! blob never collides with regular per-stage checkpoints or with the window
//! accumulator's [`ACCUMULATOR_STAGE_SENTINEL`](crate::distributed::checkpoint::ACCUMULATOR_STAGE_SENTINEL).
//!
//! ## Chained keying
//!
//! The blob is written under the current claim's id and read back under the id of the
//! claim that last wrote one; the runner holds that pointer for its whole loop. A
//! stable per-partition key is impossible: the state machine resolves a checkpoint's
//! `batch_id` from the CLAIMS table (`apply_checkpoint`), so a write whose uuid is not
//! a live claim is rejected with "claim_id not found". Reads are point lookups and
//! survive the claim being acked, so chaining works in the direction that matters.
//!
//! The blob is durable, the pointer to the newest one is not. A restarted runner hands
//! the runtime `None`, the cold start defined by the WIT `prior: option<checkpoint>`
//! contract, and state is rebuilt rather than mixed.
//!
//! ## Payload format
//!
//! The host defines none. The bytes are exactly what
//! [`PipelineRuntime::run_on_with_state`](saci_core::runtime::PipelineRuntime::run_on_with_state)
//! returned, handed back verbatim as the next call's `prior`. For a WASM processor built
//! with `saci_processor::export_pipeline!(build, state = C)` they are a single-component
//! Arrow IPC stream, but nothing here depends on that.

use uuid::Uuid;

use crate::SaciResult;
use crate::distributed::checkpoint::{CheckpointStore, PROCESSOR_STATE_STAGE_SENTINEL};
/// Load the runtime state blob written under `prior_claim_id`.
///
/// Returns `Ok(None)` on the first run, when no checkpoint has been written
/// under the sentinel, or when the stored payload is empty.
pub async fn load_processor_state(
    store: &(impl CheckpointStore + ?Sized),
    prior_claim_id: Uuid,
) -> SaciResult<Option<Vec<u8>>> {
    let checkpoint = store
        .load_checkpoint(prior_claim_id, PROCESSOR_STATE_STAGE_SENTINEL)
        .await?;

    match checkpoint {
        None => Ok(None),
        Some(cp) if cp.payload.is_empty() => Ok(None),
        Some(cp) => Ok(Some(cp.payload)),
    }
}

/// Persist the runtime state `blob` under `claim_id`, which must be a claim
/// this runner currently holds.
///
/// `schema_id` identifies the dataset shape the blob belongs to; callers pass
/// `data.schemas().fingerprint()` so a redeployed pipeline with a different
/// shape is distinguishable on disk.
///
/// Writing an empty blob is a no-op: it would be indistinguishable from "no
/// state" on the way back in, so there is nothing to record.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`](crate::SaciError::Configuration) if the
/// blob is at or above the store's
/// [`max_checkpoint_bytes`](CheckpointStore::max_checkpoint_bytes) cap, which
/// the Raft propose boundary would reject anyway.
pub async fn save_processor_state(
    store: &(impl CheckpointStore + ?Sized),
    claim_id: Uuid,
    blob: &[u8],
    schema_id: u32,
) -> SaciResult<()> {
    use crate::SaciError;

    if blob.is_empty() {
        return Ok(());
    }

    let cap = store.max_checkpoint_bytes();
    if blob.len() >= cap {
        return Err(SaciError::configuration(format!(
            "processor state blob size {} bytes exceeds the store cap {cap} — \
             reduce the runtime's retained state or shorten batches",
            blob.len(),
        )));
    }

    store
        .save_checkpoint(
            claim_id,
            PROCESSOR_STATE_STAGE_SENTINEL,
            blob.to_vec(),
            schema_id,
        )
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::checkpoint::{ACCUMULATOR_STAGE_SENTINEL, Checkpoint, CheckpointStore};
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    #[derive(Default, Clone)]
    struct MemStore {
        data: Arc<Mutex<HashMap<(Uuid, u32), Checkpoint>>>,
    }

    #[async_trait]
    impl CheckpointStore for MemStore {
        async fn save_checkpoint(
            &self,
            claim_id: Uuid,
            stage_idx: u32,
            ipc_bytes: Vec<u8>,
            schema_id: u32,
        ) -> SaciResult<()> {
            self.data.lock().unwrap().insert(
                (claim_id, stage_idx),
                Checkpoint {
                    batch_id: 0,
                    stage_idx,
                    payload: ipc_bytes,
                    schema_id,
                    created_at: 0,
                },
            );
            Ok(())
        }

        async fn load_checkpoint(
            &self,
            claim_id: Uuid,
            stage_idx: u32,
        ) -> SaciResult<Option<Checkpoint>> {
            Ok(self
                .data
                .lock()
                .unwrap()
                .get(&(claim_id, stage_idx))
                .cloned())
        }
    }

    fn claim() -> Uuid {
        Uuid::now_v7()
    }

    #[test]
    fn sentinel_does_not_collide_with_the_accumulator_sentinel() {
        assert_ne!(PROCESSOR_STATE_STAGE_SENTINEL, ACCUMULATOR_STAGE_SENTINEL);
    }

    #[tokio::test]
    async fn load_returns_none_on_first_run() {
        let store = MemStore::default();
        assert!(
            load_processor_state(&store, claim())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn save_then_load_returns_the_blob_verbatim() {
        let store = MemStore::default();
        let id = claim();
        let blob = vec![0xde, 0xad, 0xbe, 0xef];

        save_processor_state(&store, id, &blob, 7).await.unwrap();
        assert_eq!(
            load_processor_state(&store, id).await.unwrap(),
            Some(blob.clone())
        );

        let stored = store
            .load_checkpoint(id, PROCESSOR_STATE_STAGE_SENTINEL)
            .await
            .unwrap()
            .expect("checkpoint present");
        assert_eq!(stored.schema_id, 7, "schema_id must be recorded as given");
    }

    #[tokio::test]
    async fn state_is_scoped_to_the_key_it_was_written_under() {
        let store = MemStore::default();
        let a = claim();
        let b = claim();

        save_processor_state(&store, a, &[1], 1).await.unwrap();
        assert_eq!(
            load_processor_state(&store, a).await.unwrap(),
            Some(vec![1])
        );
        assert!(
            load_processor_state(&store, b).await.unwrap().is_none(),
            "a blob written under one claim must not surface under another"
        );
    }

    #[tokio::test]
    async fn save_of_an_empty_blob_writes_nothing() {
        let store = MemStore::default();
        let id = claim();
        save_processor_state(&store, id, &[], 1).await.unwrap();
        assert!(
            store
                .load_checkpoint(id, PROCESSOR_STATE_STAGE_SENTINEL)
                .await
                .unwrap()
                .is_none(),
            "an empty blob is indistinguishable from no state; do not persist it"
        );
    }

    #[tokio::test]
    async fn stored_empty_payload_loads_as_none() {
        let store = MemStore::default();
        let id = claim();
        // A checkpoint that was written with an empty payload.
        store
            .save_checkpoint(id, PROCESSOR_STATE_STAGE_SENTINEL, Vec::new(), 1)
            .await
            .unwrap();
        assert!(load_processor_state(&store, id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn save_rejects_an_oversized_blob() {
        let store = MemStore::default();
        let blob = vec![0u8; store.max_checkpoint_bytes()];
        let err = save_processor_state(&store, claim(), &blob, 1)
            .await
            .expect_err("oversized blob must be rejected");
        let msg = err.to_string();
        assert!(msg.contains("store cap"), "got: {msg}");
    }

    #[tokio::test]
    async fn state_does_not_collide_with_accumulator_checkpoints() {
        let store = MemStore::default();
        let id = claim();
        store
            .save_checkpoint(id, ACCUMULATOR_STAGE_SENTINEL, vec![1, 2, 3], 1)
            .await
            .unwrap();
        save_processor_state(&store, id, &[9, 9], 1).await.unwrap();

        assert_eq!(
            load_processor_state(&store, id).await.unwrap(),
            Some(vec![9, 9])
        );
        assert_eq!(
            store
                .load_checkpoint(id, ACCUMULATOR_STAGE_SENTINEL)
                .await
                .unwrap()
                .expect("accumulator checkpoint present")
                .payload,
            vec![1, 2, 3]
        );
    }
}
