//! Helper functions for persisting and restoring window accumulator state.
//!
//! Both functions use [`ACCUMULATOR_STAGE_SENTINEL`] as the `stage_idx` so
//! accumulator checkpoints never collide with regular per-stage checkpoints.
//!
//! ## Payload format
//!
//! `save_accumulator_state` calls [`Dataset::write_component_ipc`], which writes only
//! the `WindowAccumulator` component. The payload is bounded by the accumulator
//! row-count rather than the full dataset size, so it stays well within the store's
//! [`max_checkpoint_bytes`](CheckpointStore::max_checkpoint_bytes) cap for typical
//! window counts.
//!
//! ## First-run bootstrap
//!
//! `load_accumulator_state` returns `Ok(None)` when no prior checkpoint exists.
//! The caller should treat `None` as an empty accumulator (zero prior rows).
//!
//! ## Chained keying
//!
//! Both functions take a bare `claim_id`, not a [`BatchClaim`](super::BatchClaim): the runner
//! chains the accumulator the same way it chains the runtime state blob
//! (`processor_state_store`), loading under the id of the claim that last
//! wrote a checkpoint and saving under the id it currently holds. A fresh
//! claim's own id never has a checkpoint yet, because the state is saved
//! before the claim is acked.

#[cfg(feature = "windows")]
use arrow_array::RecordBatch;

#[cfg(feature = "windows")]
use crate::SaciResult;
#[cfg(feature = "windows")]
use crate::component::Component;
#[cfg(feature = "windows")]
use crate::dataset::Dataset;
#[cfg(feature = "windows")]
use crate::distributed::checkpoint::{ACCUMULATOR_STAGE_SENTINEL, CheckpointStore};
#[cfg(feature = "windows")]
use crate::windows::accumulator::{CURRENT_ACCUMULATOR_VERSION, WindowAccumulator};
#[cfg(feature = "windows")]
use uuid::Uuid;

/// Load the window accumulator state persisted under `claim_id` from `store`.
///
/// The caller passes the id of the claim that last wrote an accumulator
/// checkpoint (the chain pointer), mirroring `processor_state_store`; a fresh
/// claim's own id would never have a checkpoint, because the state is saved
/// before the claim is acked.
///
/// Returns the raw `RecordBatch` (after schema migration) so the caller can
/// append it into a pipeline registered with [`WindowAccumulator`].
/// Returns `Ok(None)` on the first run when no prior checkpoint exists.
#[cfg(feature = "windows")]
pub async fn load_accumulator_state(
    store: &(impl CheckpointStore + ?Sized),
    claim_id: Uuid,
) -> SaciResult<Option<RecordBatch>> {
    let checkpoint = store
        .load_checkpoint(claim_id, ACCUMULATOR_STAGE_SENTINEL)
        .await?;

    let cp = match checkpoint {
        None => return Ok(None),
        Some(c) => c,
    };

    if cp.payload.is_empty() {
        return Ok(None);
    }

    // Decode single-component IPC.
    let dataset = Dataset::read_ipc(&mut cp.payload.as_slice())?;

    let batch = match dataset.batch_for(WindowAccumulator::name()) {
        None => return Ok(None),
        Some(b) if b.num_rows() == 0 => return Ok(None),
        Some(b) => b.clone(),
    };

    // Migrate using the version embedded in the IPC metadata.
    let on_disk_version = dataset
        .schemas()
        .get_version(WindowAccumulator::name())
        .unwrap_or(1);
    let migrated = WindowAccumulator::migrate(on_disk_version, batch)?;
    Ok(Some(migrated))
}

/// Persist the window accumulator component from `data` to `store` under
/// `claim_id`.
///
/// Serialises only the `WindowAccumulator` component (not the whole dataset).
/// The payload size is validated against the store's
/// [`max_checkpoint_bytes`](CheckpointStore::max_checkpoint_bytes) before
/// writing. If the component is not registered in `data`, the save is a no-op.
#[cfg(feature = "windows")]
pub async fn save_accumulator_state(
    store: &(impl CheckpointStore + ?Sized),
    claim_id: Uuid,
    data: &Dataset,
) -> SaciResult<()> {
    use crate::SaciError;

    // If the component isn't in this dataset there's nothing to checkpoint.
    if data.batch_for(WindowAccumulator::name()).is_none() {
        return Ok(());
    }

    let mut buf = Vec::new();
    data.write_component_ipc::<WindowAccumulator>(&mut buf)?;

    let cap = store.max_checkpoint_bytes();
    if buf.len() >= cap {
        return Err(SaciError::configuration(format!(
            "accumulator checkpoint size {} bytes exceeds the store cap {cap} — \
             reduce the number of open windows or split the pipeline",
            buf.len(),
        )));
    }

    store
        .save_checkpoint(
            claim_id,
            ACCUMULATOR_STAGE_SENTINEL,
            buf,
            CURRENT_ACCUMULATOR_VERSION,
        )
        .await
}

#[cfg(all(test, feature = "windows"))]
mod tests {
    use super::*;
    use crate::SaciResult;
    use crate::distributed::checkpoint::{Checkpoint, CheckpointStore};
    use crate::windows::accumulator::WindowAccumulator;
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

    fn make_world_with_accumulators(rows: Vec<WindowAccumulator>) -> Dataset {
        let mut data = Dataset::new();
        data.register_component::<WindowAccumulator>().unwrap();
        if !rows.is_empty() {
            data.append::<WindowAccumulator>(&rows).unwrap();
        }
        data
    }

    #[tokio::test]
    async fn test_load_returns_none_when_no_checkpoint() {
        let store = MemStore::default();
        let claim_id = Uuid::now_v7();
        let result = load_accumulator_state(&store, claim_id).await.unwrap();
        assert!(result.is_none(), "expected None on first run");
    }

    #[tokio::test]
    async fn test_save_and_load_round_trip() {
        let store = MemStore::default();
        let claim_id = Uuid::now_v7();

        let acc = WindowAccumulator {
            version: Some(1),
            source_component: "Trade".to_string(),
            window_id: 42,
            key_hash: 100,
            count: 5,
            sum_f64: Some(99.5),
            min_f64: Some(1.0),
            max_f64: Some(50.0),
            session_start_ts: None,
            session_end_ts: None,
            finalized_at_watermark: None,
        };

        let pipeline = make_world_with_accumulators(vec![acc.clone()]);
        save_accumulator_state(&store, claim_id, &pipeline)
            .await
            .unwrap();

        let loaded = load_accumulator_state(&store, claim_id).await.unwrap();
        let batch = loaded.expect("should have a batch");
        assert_eq!(batch.num_rows(), 1);

        let recovered = WindowAccumulator::from_record_batch(&batch).unwrap();
        assert_eq!(recovered[0].window_id, 42);
        assert_eq!(recovered[0].key_hash, 100);
        assert_eq!(recovered[0].sum_f64, Some(99.5));
    }

    #[tokio::test]
    async fn test_save_empty_world_is_noop() {
        let store = MemStore::default();
        let claim_id = Uuid::now_v7();

        // Dataset without WindowAccumulator registered.
        let data = Dataset::new();
        save_accumulator_state(&store, claim_id, &data)
            .await
            .unwrap();

        let loaded = load_accumulator_state(&store, claim_id).await.unwrap();
        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn test_save_uses_sentinel_stage_idx() {
        let store = MemStore::default();
        let claim_id = Uuid::now_v7();

        let pipeline = make_world_with_accumulators(vec![WindowAccumulator {
            version: Some(1),
            source_component: "X".to_string(),
            window_id: 0,
            key_hash: 0,
            count: 1,
            sum_f64: Some(1.0),
            min_f64: Some(1.0),
            max_f64: Some(1.0),
            session_start_ts: None,
            session_end_ts: None,
            finalized_at_watermark: None,
        }]);
        save_accumulator_state(&store, claim_id, &pipeline)
            .await
            .unwrap();

        // The checkpoint is stored at the sentinel key.
        let cp = store
            .load_checkpoint(claim_id, ACCUMULATOR_STAGE_SENTINEL)
            .await
            .unwrap();
        assert!(cp.is_some(), "should have a checkpoint at the sentinel");
        assert_eq!(cp.unwrap().schema_id, CURRENT_ACCUMULATOR_VERSION);
    }

    #[tokio::test]
    async fn test_save_registered_but_empty_component_loads_none() {
        let store = MemStore::default();
        let claim_id = Uuid::now_v7();

        // Registered but no rows appended.
        let pipeline = make_world_with_accumulators(vec![]);
        save_accumulator_state(&store, claim_id, &pipeline)
            .await
            .unwrap();

        let loaded = load_accumulator_state(&store, claim_id).await.unwrap();
        assert!(loaded.is_none(), "empty accumulator should load as None");
    }
}
