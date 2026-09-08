//! In-memory `PartitionSource` + `CheckpointStore` fixture.
//!
//! The runner suites need a store whose claim bookkeeping is real but whose
//! backing is a `Mutex<HashMap>`, so they can assert runner semantics — claim,
//! renew, ack, release, checkpoint resume — without a cluster. Semantics
//! mirror `RedbSharedStore`: one claimable row range per registered batch,
//! `Pending → Claimed → Completed`, and an expired lease is reclaimable.
//!
//! ```rust,ignore
//! let store = MemoryStore::new();
//! store.register_master_batch(0, "Order".into(), 1, ipc, 10).await?;
//! let claim = store.claim_next_batch(instance).await?.unwrap();
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use saci_service::distributed::checkpoint::{
    ACCUMULATOR_STAGE_SENTINEL, Checkpoint, CheckpointStore, PROCESSOR_STATE_STAGE_SENTINEL,
};
use saci_service::distributed::partition::{BatchClaim, PartitionSource};
use saci_service::{SaciError, SaciResult};
use uuid::Uuid;

/// Lease TTL a freshly built [`MemoryStore`] grants, in milliseconds.
pub const DEFAULT_LEASE_TTL_MILLIS: u64 = 30_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RangeStatus {
    Pending,
    Claimed,
    Completed,
}

/// One registered master batch and the state of its single row range.
struct BatchState {
    batch_id: u64,
    component: String,
    schema_id: u32,
    total_rows: u32,
    status: RangeStatus,
    claim_id: Uuid,
    instance_id: Uuid,
    lease_expires_at: u64,
}

#[derive(Default)]
struct Inner {
    /// Registration order is claim order, like a key scan over batch ids.
    batches: Vec<BatchState>,
    checkpoints: HashMap<(Uuid, u32), Checkpoint>,
    /// Schema id of the newest data-stage checkpoint.
    persisted_schema_id: Option<u32>,
}

/// A shared store held entirely in memory.
///
/// Cloning shares one set of batches and checkpoints, so several runners can
/// contend for the same work the way they do against a real store.
#[derive(Clone)]
pub struct MemoryStore {
    lease_ttl_millis: u64,
    inner: Arc<Mutex<Inner>>,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl MemoryStore {
    /// A store granting [`DEFAULT_LEASE_TTL_MILLIS`] leases.
    pub fn new() -> Self {
        Self {
            lease_ttl_millis: DEFAULT_LEASE_TTL_MILLIS,
            inner: Arc::new(Mutex::new(Inner::default())),
        }
    }

    /// Register a master batch whose whole row range becomes claimable.
    ///
    /// The payload is dropped: the runner builds each partition dataset from
    /// its own world factory and never reads the master batch back.
    pub async fn register_master_batch(
        &self,
        batch_id: u64,
        component: String,
        schema_id: u32,
        _ipc_bytes: Vec<u8>,
        total_rows: u32,
    ) -> SaciResult<()> {
        let mut inner = self.lock();
        if inner.batches.iter().any(|b| b.batch_id == batch_id) {
            return Err(SaciError::configuration(format!(
                "master batch {batch_id} already registered"
            )));
        }
        inner.batches.push(BatchState {
            batch_id,
            component,
            schema_id,
            total_rows,
            status: RangeStatus::Pending,
            claim_id: Uuid::nil(),
            instance_id: Uuid::nil(),
            lease_expires_at: 0,
        });
        Ok(())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[async_trait]
impl PartitionSource for MemoryStore {
    async fn claim_next_batch(&self, instance_id: Uuid) -> SaciResult<Option<BatchClaim>> {
        let now = now_millis();
        let ttl = self.lease_ttl_millis;
        let mut inner = self.lock();
        let Some(batch) = inner.batches.iter_mut().find(|b| match b.status {
            RangeStatus::Pending => true,
            RangeStatus::Claimed => b.lease_expires_at < now,
            RangeStatus::Completed => false,
        }) else {
            return Ok(None);
        };
        let claim_id = Uuid::now_v7();
        batch.status = RangeStatus::Claimed;
        batch.claim_id = claim_id;
        batch.instance_id = instance_id;
        batch.lease_expires_at = now + ttl;
        Ok(Some(BatchClaim {
            batch_id: batch.batch_id,
            component: batch.component.clone(),
            row_range: 0..batch.total_rows,
            schema_id: batch.schema_id,
            claim_id,
            instance_id,
            lease_expires_at: batch.lease_expires_at,
            lease_ttl_millis: ttl,
            claimed_at: Instant::now(),
        }))
    }

    async fn renew_claim(&self, claim_id: Uuid, instance_id: Uuid) -> SaciResult<u64> {
        let expires_at = now_millis() + self.lease_ttl_millis;
        let mut inner = self.lock();
        let batch = find_claim(&mut inner, claim_id, instance_id)?;
        batch.lease_expires_at = expires_at;
        Ok(expires_at)
    }

    async fn ack_claim(&self, claim_id: Uuid, instance_id: Uuid) -> SaciResult<()> {
        let mut inner = self.lock();
        let batch = find_claim(&mut inner, claim_id, instance_id)?;
        batch.status = RangeStatus::Completed;
        batch.lease_expires_at = 0;
        Ok(())
    }

    async fn release_claim(&self, claim_id: Uuid, instance_id: Uuid) -> SaciResult<()> {
        let mut inner = self.lock();
        let batch = find_claim(&mut inner, claim_id, instance_id)?;
        batch.status = RangeStatus::Pending;
        batch.claim_id = Uuid::nil();
        batch.instance_id = Uuid::nil();
        batch.lease_expires_at = 0;
        Ok(())
    }

    async fn reclaim_expired(&self, now_millis: u64) -> SaciResult<u32> {
        let mut inner = self.lock();
        let mut freed = 0u32;
        for batch in &mut inner.batches {
            if batch.status == RangeStatus::Claimed && batch.lease_expires_at < now_millis {
                batch.status = RangeStatus::Pending;
                batch.claim_id = Uuid::nil();
                batch.instance_id = Uuid::nil();
                batch.lease_expires_at = 0;
                freed += 1;
            }
        }
        Ok(freed)
    }
}

#[async_trait]
impl CheckpointStore for MemoryStore {
    async fn save_checkpoint(
        &self,
        claim_id: Uuid,
        stage_idx: u32,
        ipc_bytes: Vec<u8>,
        schema_id: u32,
    ) -> SaciResult<()> {
        let mut inner = self.lock();
        let batch_id = inner
            .batches
            .iter()
            .find(|b| b.claim_id == claim_id)
            .map(|b| b.batch_id)
            .ok_or_else(|| SaciError::generic(format!("unknown claim {claim_id}")))?;
        let is_data_stage =
            stage_idx != ACCUMULATOR_STAGE_SENTINEL && stage_idx != PROCESSOR_STATE_STAGE_SENTINEL;
        inner.checkpoints.insert(
            (claim_id, stage_idx),
            Checkpoint {
                batch_id,
                stage_idx,
                payload: ipc_bytes,
                schema_id,
                created_at: now_millis(),
            },
        );
        if is_data_stage {
            inner.persisted_schema_id = Some(schema_id);
        }
        Ok(())
    }

    async fn load_checkpoint(
        &self,
        claim_id: Uuid,
        stage_idx: u32,
    ) -> SaciResult<Option<Checkpoint>> {
        Ok(self.lock().checkpoints.get(&(claim_id, stage_idx)).cloned())
    }

    async fn persisted_schema_id(&self) -> SaciResult<Option<u32>> {
        Ok(self.lock().persisted_schema_id)
    }
}

/// Resolve a claim id to its row range, rejecting an unknown claim, a claim
/// held by another instance, and a range that is no longer `Claimed`.
fn find_claim(inner: &mut Inner, claim_id: Uuid, instance_id: Uuid) -> SaciResult<&mut BatchState> {
    let batch = inner
        .batches
        .iter_mut()
        .find(|b| b.claim_id == claim_id)
        .ok_or_else(|| SaciError::generic(format!("unknown claim {claim_id}")))?;
    if batch.instance_id != instance_id {
        return Err(SaciError::generic(format!(
            "claim {claim_id} is held by another instance"
        )));
    }
    if batch.status != RangeStatus::Claimed {
        return Err(SaciError::generic(format!(
            "claim {claim_id} is not in Claimed state"
        )));
    }
    Ok(batch)
}
