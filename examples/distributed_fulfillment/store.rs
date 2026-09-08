//! `FulfillmentStore`: a thin wrapper around [`RedbSharedStore`].
//!
//! [`DistributedRunner`] calls `world_factory()` for a fresh [`Dataset`] but
//! never loads the master-batch IPC that the generator registered. This wrapper
//! intercepts [`PartitionSource::claim_next_batch`], reads those bytes from the
//! Raft state-machine database, and stashes them in a shared slot that
//! `world_factory` drains to hydrate real `Order` rows.

use std::io::Cursor;
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use saci_service::SaciError;
use saci_service::SaciResult;
use saci_service::component::Component;
use saci_service::dataset::Dataset;
use saci_service::distributed::RedbSharedStore;
use saci_service::distributed::checkpoint::{Checkpoint, CheckpointStore};
use saci_service::distributed::consensus::state_machine::read_master_batch;
use saci_service::distributed::partition::{BatchClaim, PartitionSource};

use crate::components::{Invoice, Order};
use crate::resources::{FxRateTable, InventoryCatalog, NodeId, TaxRateTable};

/// Wraps [`RedbSharedStore`], intercepting `claim_next_batch` to pre-load the
/// master-batch Arrow IPC into a shared slot so `world_factory` can hydrate
/// the dataset with real `Order` rows.
#[derive(Clone)]
pub struct FulfillmentStore {
    /// Underlying store for all partition and checkpoint operations.
    pub inner: Arc<RedbSharedStore>,
    /// Read-only handle on the Raft state-machine database, the same `Arc` the
    /// state machine owns. Passed to `read_master_batch`.
    app_db: Arc<std::sync::Mutex<redb::Database>>,
    /// IPC bytes stashed by `claim_next_batch`, consumed by `world_factory`.
    pending_world_ipc: Arc<std::sync::Mutex<Option<Vec<u8>>>>,
}

impl FulfillmentStore {
    /// Construct a store. `app_db` must be the same `Arc<Mutex<Database>>` passed
    /// to [`RedbSharedStore::multi_node`], so reads see the same committed data.
    pub fn new(inner: Arc<RedbSharedStore>, app_db: Arc<std::sync::Mutex<redb::Database>>) -> Self {
        Self {
            inner,
            app_db,
            pending_world_ipc: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Register a master batch through the inner store. The generator calls this
    /// to push new `Order` batches into the cluster.
    pub async fn register_batch(
        &self,
        batch_id: u64,
        component: String,
        schema_id: u32,
        ipc_bytes: Vec<u8>,
        total_rows: u32,
    ) -> SaciResult<()> {
        self.inner
            .register_master_batch(batch_id, component, schema_id, ipc_bytes, total_rows)
            .await
    }

    /// Return a `Fn() -> Dataset` closure for [`DistributedRunner::run`].
    ///
    /// The closure drains the IPC bytes stashed by `claim_next_batch`, hydrates
    /// a dataset holding `Order` and `Invoice`, and inserts the resource tables.
    /// An empty slot, as on the first call before any claim, yields an empty
    /// dataset.
    pub fn world_factory(&self, node_id: u64) -> impl Fn() -> Dataset {
        let pending = Arc::clone(&self.pending_world_ipc);
        move || {
            // Components must be registered before rows are appended, so the
            // dataset from `Dataset::read_ipc` cannot be reused: it already
            // carries Order rows.
            let mut dataset = Dataset::builder().with::<Order>().with::<Invoice>().build();

            if let Some(ipc) = pending.lock().unwrap().take() {
                let mut cursor = Cursor::new(ipc);
                let tmp_world = Dataset::read_ipc(&mut cursor)
                    .expect("FulfillmentStore: corrupted master-batch IPC");
                if let Some(batch) = tmp_world.columns::<Order>() {
                    let orders = Order::from_record_batch(batch)
                        .expect("FulfillmentStore: decode Order RecordBatch");
                    if !orders.is_empty() {
                        dataset
                            .append::<Order>(&orders)
                            .expect("FulfillmentStore: append Order rows");
                    }
                }
            }

            // Resources are stateless lookups; always insert defaults.
            dataset.insert_resource(FxRateTable::default());
            dataset.insert_resource(TaxRateTable::default());
            dataset.insert_resource(InventoryCatalog::default());
            dataset.insert_resource(NodeId(node_id));
            dataset
        }
    }
}

#[async_trait]
impl PartitionSource for FulfillmentStore {
    /// Claim the next pending batch and stash its IPC bytes in the shared slot
    /// so the pipeline factory can hydrate real `Order` rows.
    async fn claim_next_batch(&self, instance_id: Uuid) -> SaciResult<Option<BatchClaim>> {
        let claim_opt = self.inner.claim_next_batch(instance_id).await?;

        if let Some(ref claim) = claim_opt {
            let batch_id = claim.batch_id;
            let db = Arc::clone(&self.app_db);

            // The DB read blocks, so it runs on the blocking pool.
            let ipc_opt = tokio::task::spawn_blocking(move || {
                let db = db
                    .lock()
                    .map_err(|_| SaciError::store("app_db mutex poisoned"))?;
                let record = read_master_batch(&db, batch_id)?;
                Ok::<Option<Vec<u8>>, SaciError>(record.map(|r| r.ipc_bytes))
            })
            .await
            .map_err(|e| SaciError::generic(format!("spawn_blocking: {e}")))??;

            *self.pending_world_ipc.lock().unwrap() = ipc_opt;

            #[cfg(feature = "tracing")]
            tracing::debug!(
                batch_id,
                "FulfillmentStore: stashed master-batch IPC for pipeline factory"
            );
        }

        Ok(claim_opt)
    }

    async fn renew_claim(&self, claim_id: Uuid, instance_id: Uuid) -> SaciResult<u64> {
        self.inner.renew_claim(claim_id, instance_id).await
    }

    async fn ack_claim(&self, claim_id: Uuid, instance_id: Uuid) -> SaciResult<()> {
        self.inner.ack_claim(claim_id, instance_id).await
    }

    async fn release_claim(&self, claim_id: Uuid, instance_id: Uuid) -> SaciResult<()> {
        self.inner.release_claim(claim_id, instance_id).await
    }
}

#[async_trait]
impl CheckpointStore for FulfillmentStore {
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
