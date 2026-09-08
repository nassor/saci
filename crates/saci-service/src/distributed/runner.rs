//! [`DistributedRunner`]: orchestration loop for distributed Arrow batch execution.
//!
//! For each claimed batch the runner builds a fresh [`Dataset`] from `world_factory()`,
//! loads the prior window accumulator plus the runtime's opaque state blob, calls
//! `runtime.run_on_with_state`, saves a checkpoint, then acks the claim. The runtime is a
//! template reused across batches: it keeps no per-batch state, and its own data, sources,
//! and sinks go unused because partition data arrives via [`PartitionSource`].
//!
//! Lease renewal is checked before execution. If [`PartitionSource::renew_claim`] fails the
//! runner stops and returns an error; continuing past a lease failure would violate
//! at-most-once semantics.
//!
//! A batch whose state cannot be persisted is never acked. On a checkpoint or state-save
//! failure the runner calls [`PartitionSource::release_claim`] so the next runner retries
//! it, and a dataset larger than [`MAX_LOG_ENTRY_BYTES`] fails with
//! [`SaciError::configuration`] rather than being acked with partial state.
//!
//! Each claimed batch opens a `workflow.batch` root span holding one
//! `runtime.run`, so one claim is one trace in the dashboard. `runtime.run` is
//! the contextual parent of whatever the runtime opens: `pipeline.run` for a
//! native pipeline, `processor.batch` for a processor component or a native
//! plugin.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use saci_core::PipelineRuntime;

use crate::SaciError;
use crate::SaciResult;
use crate::dataset::Dataset;
use crate::distributed::checkpoint::CheckpointStore;
use crate::distributed::partition::{MAX_LOG_ENTRY_BYTES, PartitionSource};
use crate::distributed::strategy::CheckpointStrategy;
#[cfg(test)]
use crate::pipeline::Pipeline;

pub use crate::partition::KeyPartition;

/// Configuration for an [`DistributedRunner`].
#[derive(Debug, Clone)]
pub struct RunnerConfig {
    /// Unique identifier for this runner instance.
    pub instance_id: Uuid,
    /// Checkpoint frequency.
    pub checkpoint_strategy: CheckpointStrategy,
    /// Default schema version applied when writing checkpoints.
    pub schema_id: u32,
    /// Maximum number of batches to process (useful for testing).
    pub max_batches: Option<usize>,
    /// How frequently to check lease renewal (in milliseconds).
    pub lease_renewal_check_interval_millis: u64,
    /// The workflow this runner drives, for the `workflow.batch` root span.
    /// Defaults to an empty string so existing library tests keep compiling.
    pub workflow_id: String,
    /// The declared id of the one processor node cluster mode runs, for
    /// `runtime.run`'s `processor` field. Defaults to an empty string so
    /// existing library tests keep compiling.
    pub processor_id: String,
    /// Optional key-based partition mask for multi-instance window accumulation.
    ///
    /// When `Some`, the runner injects a [`KeyPartition`] resource that `WindowedSystem`
    /// reads to filter out rows belonging to other instances. `None`, the default, lets
    /// every runner process every row.
    #[cfg(feature = "windows")]
    pub partition_mask: Option<KeyPartition>,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            instance_id: Uuid::now_v7(),
            checkpoint_strategy: CheckpointStrategy::EveryStage,
            schema_id: 1,
            max_batches: None,
            lease_renewal_check_interval_millis: 5_000,
            workflow_id: String::new(),
            processor_id: String::new(),
            #[cfg(feature = "windows")]
            partition_mask: None,
        }
    }
}

/// Distributed runner that claims Arrow row-range batches, runs them through a
/// [`PipelineRuntime`] template, checkpoints intermediate state, and acks on completion.
///
/// Per-partition [`Dataset`]s arrive via [`PartitionSource`]; the runtime's own sources
/// and sinks are unused in distributed mode.
///
/// ```no_run
/// # #[cfg(feature = "distributed")]
/// # {
/// use saci_service::distributed::runner::{DistributedRunner, RunnerConfig};
///
/// async fn example() {}
/// # }
/// ```
pub struct DistributedRunner<S> {
    store: S,
    runtime: Box<dyn PipelineRuntime>,
    config: RunnerConfig,
}

impl<S> DistributedRunner<S>
where
    S: PartitionSource + CheckpointStore,
{
    /// Create a runner that drives `runtime` once per claimed batch.
    ///
    /// Per-partition data comes from `world_factory` on each call to [`run`](Self::run).
    pub fn new(store: S, runtime: Box<dyn PipelineRuntime>, config: RunnerConfig) -> Self {
        Self {
            store,
            runtime,
            config,
        }
    }

    /// Run the processing loop until `max_batches` is reached, if configured, or no more
    /// batches are available.
    ///
    /// `world_factory` produces a fresh, empty [`Dataset`] per batch; register components
    /// and resources there. Lease renewal runs concurrently with execution at `TTL/3`
    /// cadence, and a mid-execution renewal failure cancels the run, releases the claim,
    /// and continues with the next batch.
    ///
    /// # Errors
    ///
    /// Returns the first hard [`SaciError`] encountered.
    pub async fn run(&self, world_factory: impl Fn() -> Dataset) -> SaciResult<usize> {
        self.run_with_shutdown(world_factory, CancellationToken::new())
            .await
    }

    /// Like [`run`](Self::run) but accepts a [`CancellationToken`] for graceful shutdown.
    ///
    /// The loop exits between batches, never mid-batch, so no lease is held on exit.
    pub async fn run_with_shutdown(
        &self,
        world_factory: impl Fn() -> Dataset,
        shutdown: CancellationToken,
    ) -> SaciResult<usize> {
        let mut processed = 0usize;

        // Recycles orphaned leases from crashed runners without operator intervention.
        // The deadline check is non-blocking, so it fires as the loop iterates between
        // batches.
        let sweep_interval = Duration::from_secs(30); // ≈ default TTL (90 s) / 3
        let mut next_sweep = std::time::Instant::now() + sweep_interval;

        // Claim id under which this runner last persisted the runtime's state blob. See
        // `processor_state_store` for why the pointer is chained rather than derived from a
        // stable partition key.
        let mut state_claim_id: Option<Uuid> = None;
        // Note: a *process restart* still re-claims with fresh UUIDs, so
        // cross-restart accumulator resume stays claim-granular (unchanged
        // redb-era semantics); within a run, chaining now works. Only the
        // windowed arms below load from and advance this pointer, so without
        // `windows` there is nothing to chain and no binding to declare.
        #[cfg(all(feature = "windows", feature = "distributed"))]
        let mut accumulator_claim_id: Option<Uuid> = None;

        loop {
            if let Some(max) = self.config.max_batches
                && processed >= max
            {
                break;
            }

            // No claim is held between batches, so this is a clean exit point.
            if shutdown.is_cancelled() {
                break;
            }

            let now_instant = std::time::Instant::now();
            if now_instant >= next_sweep {
                next_sweep = now_instant + sweep_interval;
                match self.store.reclaim_expired(Self::now_millis()).await {
                    Ok(n) => {
                        #[cfg(feature = "tracing")]
                        if n > 0 {
                            tracing::info!(reclaimed_count = n, "swept expired leases");
                        }
                        #[cfg(not(feature = "tracing"))]
                        let _ = n;
                    }
                    Err(e) => {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(error = %e, "reclaim_expired sweep failed");
                        #[cfg(not(feature = "tracing"))]
                        let _ = e;
                    }
                }
            }

            let claim = match self.store.claim_next_batch(self.config.instance_id).await? {
                None => break,
                Some(c) => c,
            };

            // One root span per claimed batch, which is one trace in the
            // dashboard. Children are created inside
            // `batch_span.in_scope(...)`, so the batch span is their
            // contextual parent, which is the only form the subscriber's
            // sampler can follow; `in_scope` is synchronous, and an entered
            // guard held across this body's awaits would adopt every span the
            // runtime opens on this thread meanwhile.
            // `debug`, not `info`: the same per-item tree the standalone and
            // stream runners open, so it carries the same level and every
            // runner is treated alike. The default `log_level="error"`
            // materialises no span at all; `log_level="debug"` brings the
            // per-claim traces back.
            #[cfg(feature = "tracing")]
            let batch_span = tracing::debug_span!(
                "workflow.batch",
                workflow = %self.config.workflow_id,
                claim = %claim.claim_id,
                rows = tracing::field::Empty
            );

            let mut partition_data = world_factory();

            #[cfg(all(feature = "windows", feature = "distributed"))]
            {
                if let Some(kp) = self.config.partition_mask {
                    partition_data.insert_resource(kp);
                }

                use crate::component::Component as _;
                use crate::distributed::accumulator_store::load_accumulator_state;
                use crate::windows::accumulator::WindowAccumulator;

                // Load under the claim that last wrote an accumulator
                // checkpoint, mirroring the processor-state chain; a fresh
                // claim starts cold: its own id has no checkpoint yet.
                match accumulator_claim_id {
                    None => {}
                    Some(prior_id) => match load_accumulator_state(&self.store, prior_id).await {
                        Ok(Some(batch)) => {
                            if partition_data
                                .batch_for(WindowAccumulator::name())
                                .is_some()
                            {
                                let rows =
                                    WindowAccumulator::from_record_batch(&batch).map_err(|e| {
                                        SaciError::generic(format!(
                                            "DistributedRunner: failed to decode accumulator: {e}"
                                        ))
                                    })?;
                                partition_data
                                        .append::<WindowAccumulator>(&rows)
                                        .map_err(|e| {
                                            SaciError::generic(format!(
                                                "DistributedRunner: failed to restore accumulator rows: {e}"
                                            ))
                                        })?;
                            }
                        }
                        Ok(None) => {}
                        Err(_e) => {
                            #[cfg(feature = "tracing")]
                            tracing::error!(
                                parent: &batch_span,
                                workflow = %self.config.workflow_id,
                                claim_id = %claim.claim_id,
                                error = %_e,
                                "accumulator load failed; releasing claim for retry"
                            );
                            Self::release_with_log(&self.store, &claim).await;
                            continue;
                        }
                    },
                }
            }

            // Opaque to the host: whatever `run_on_with_state` returned on this runner's
            // previous batch, handed straight back.
            let prior_state = match state_claim_id {
                None => None,
                Some(prior_id) => {
                    match crate::distributed::processor_state_store::load_processor_state(
                        &self.store,
                        prior_id,
                    )
                    .await
                    {
                        Ok(blob) => blob,
                        Err(_e) => {
                            #[cfg(feature = "tracing")]
                            tracing::error!(
                                parent: &batch_span,
                                workflow = %self.config.workflow_id,
                                claim_id = %claim.claim_id,
                                error = %_e,
                                "processor state load failed; releasing claim for retry"
                            );
                            Self::release_with_log(&self.store, &claim).await;
                            continue;
                        }
                    }
                }
            };

            if self.store.should_renew(&claim) {
                match self
                    .store
                    .renew_claim(claim.claim_id, claim.instance_id)
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        Self::release_with_log(&self.store, &claim).await;
                        return Err(SaciError::generic(format!(
                            "lease renewal failed for claim {}: {e}",
                            claim.claim_id
                        )));
                    }
                }
            }

            // Renewal is a sibling select branch, so tokio polls it concurrently with
            // `run_on`. When renewal fails its branch resolves first, and dropping the
            // select cancels `run_on` at its next `.await`.
            let renewal_interval = Duration::from_millis((claim.lease_ttl_millis / 3).max(1));
            let claim_id = claim.claim_id;
            let claim_instance_id = claim.instance_id;
            let store_ref = &self.store;
            // Borrowed as a local for the same reason as `store_ref`: the async
            // block must not capture `self`.
            #[cfg(feature = "tracing")]
            let workflow_id_for_renewal = self.config.workflow_id.as_str();
            let renewal_branch = async {
                loop {
                    tokio::time::sleep(renewal_interval).await;
                    if let Err(e) = store_ref.renew_claim(claim_id, claim_instance_id).await {
                        #[cfg(feature = "tracing")]
                        tracing::error!(
                            parent: &batch_span,
                            workflow = %workflow_id_for_renewal,
                            %claim_id,
                            error = %e,
                            "mid-execution lease renewal failed; cancelling run_on"
                        );
                        #[cfg(not(feature = "tracing"))]
                        let _ = e;
                        return;
                    }
                }
            };

            crate::metrics::instruments().source_batch(&self.config.processor_id);
            crate::metrics::instruments()
                .rows(&self.config.processor_id, partition_data.rows() as u64);

            #[cfg(feature = "tracing")]
            batch_span.record("rows", partition_data.rows() as u64);

            enum RunOutcome {
                Ran(SaciResult<Option<Vec<u8>>>),
                RenewalFailed,
            }
            let runtime = &*self.runtime;
            // The contextual parent of whatever the runtime opens: `pipeline.run`
            // for a native pipeline, `processor.batch` for a processor component
            // or a native plugin.
            #[cfg(feature = "tracing")]
            let run_span = batch_span.in_scope(|| {
                tracing::debug_span!(
                    "runtime.run",
                    workflow = %self.config.workflow_id,
                    processor = %self.config.processor_id,
                    runtime = runtime.name(),
                    rows_in = partition_data.rows() as u64,
                    rows_out = tracing::field::Empty
                )
            });
            let run = runtime.run_on_with_state(&mut partition_data, prior_state.as_deref());
            #[cfg(feature = "tracing")]
            let run = tracing::Instrument::instrument(run, run_span.clone());
            let outcome = tokio::select! {
                biased;
                result = run => RunOutcome::Ran(result),
                () = renewal_branch => RunOutcome::RenewalFailed,
            };

            #[cfg(feature = "tracing")]
            run_span.record("rows_out", partition_data.rows() as u64);

            // A span closes when its last handle drops, so `runtime.run` has to
            // go before the checkpoint and state writes or it would time those
            // too.
            #[cfg(feature = "tracing")]
            drop(run_span);

            let run_result: SaciResult<Option<Vec<u8>>> = match outcome {
                RunOutcome::Ran(r) => r,
                RunOutcome::RenewalFailed => {
                    crate::metrics::instruments().workflow_error(&self.config.workflow_id);
                    Self::release_with_log(&self.store, &claim).await;
                    continue;
                }
            };

            let (next_state, mut run_error) = match run_result {
                Ok(blob) => (blob, None),
                Err(e) => (None, Some(e)),
            };
            let mut claim_released = false;

            if run_error.is_none()
                && self.config.checkpoint_strategy.should_checkpoint(0)
                && let Err(e) = self
                    .write_checkpoint(&claim.claim_id, 0, &partition_data)
                    .await
            {
                #[cfg(feature = "tracing")]
                tracing::error!(
                    parent: &batch_span,
                    workflow = %self.config.workflow_id,
                    claim_id = %claim.claim_id,
                    error = %e,
                    "checkpoint save failed; releasing claim for retry"
                );
                Self::release_with_log(&self.store, &claim).await;
                claim_released = true;
                run_error = Some(e);
            }

            #[cfg(all(feature = "windows", feature = "distributed"))]
            if run_error.is_none() && !claim_released {
                use crate::distributed::accumulator_store::save_accumulator_state;
                match save_accumulator_state(&self.store, claim.claim_id, &partition_data).await {
                    // Advance the chain only once the save is durable, same
                    // contract as the processor-state pointer below.
                    Ok(()) => accumulator_claim_id = Some(claim.claim_id),
                    Err(e) => {
                        #[cfg(feature = "tracing")]
                        tracing::error!(
                            parent: &batch_span,
                            workflow = %self.config.workflow_id,
                            claim_id = %claim.claim_id,
                            error = %e,
                            "accumulator save failed; releasing claim for retry"
                        );
                        Self::release_with_log(&self.store, &claim).await;
                        claim_released = true;
                        run_error = Some(e);
                    }
                }
            }

            if run_error.is_none()
                && !claim_released
                && let Some(blob) = next_state.as_deref()
            {
                match crate::distributed::processor_state_store::save_processor_state(
                    &self.store,
                    claim.claim_id,
                    blob,
                    partition_data.schemas().fingerprint(),
                )
                .await
                {
                    // Advance the chain only once the blob is durable: a failed save
                    // leaves the previous state readable.
                    Ok(()) => state_claim_id = Some(claim.claim_id),
                    Err(e) => {
                        #[cfg(feature = "tracing")]
                        tracing::error!(
                            parent: &batch_span,
                            workflow = %self.config.workflow_id,
                            claim_id = %claim.claim_id,
                            error = %e,
                            "processor state save failed; releasing claim for retry"
                        );
                        Self::release_with_log(&self.store, &claim).await;
                        claim_released = true;
                        run_error = Some(e);
                    }
                }
            }

            match (run_error, claim_released) {
                (Some(e), true) => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        parent: &batch_span,
                        workflow = %self.config.workflow_id,
                        claim_id = %claim.claim_id,
                        error = %e,
                        "skipping ack: claim was released on a post-run persist failure"
                    );
                    crate::metrics::instruments().workflow_error(&self.config.workflow_id);
                    let _ = e;
                    continue;
                }
                (Some(e), false) => {
                    Self::release_with_log(&self.store, &claim).await;
                    crate::metrics::instruments().workflow_error(&self.config.workflow_id);
                    return Err(e);
                }
                (None, _) => {
                    self.store
                        .ack_claim(claim.claim_id, claim.instance_id)
                        .await?;
                    processed += 1;
                    crate::metrics::instruments().workflow_run(&self.config.workflow_id);
                }
            }
        }

        Ok(processed)
    }

    async fn release_with_log(store: &S, claim: &crate::distributed::partition::BatchClaim) {
        if let Err(e) = store.release_claim(claim.claim_id, claim.instance_id).await {
            #[cfg(feature = "tracing")]
            tracing::error!(
                claim_id = %claim.claim_id,
                error = %e,
                "release_claim failed; claim may be orphaned until lease expiry"
            );
            // Log even without the tracing feature so the error isn't silently swallowed.
            #[cfg(not(feature = "tracing"))]
            eprintln!("release_claim failed for {}: {e}", claim.claim_id);
        }
    }

    /// Serialize `data` as Arrow IPC and write a checkpoint.
    ///
    /// The serialized dataset must fit within [`MAX_LOG_ENTRY_BYTES`]; anything larger is
    /// a [`SaciError::configuration`] error, never a truncated or empty checkpoint.
    async fn write_checkpoint(
        &self,
        claim_id: &Uuid,
        stage_idx: u32,
        data: &Dataset,
    ) -> SaciResult<()> {
        let mut buf = Vec::new();
        data.write_ipc(&mut buf)?;

        if buf.len() > MAX_LOG_ENTRY_BYTES {
            return Err(SaciError::configuration(format!(
                "checkpoint dataset size {} bytes exceeds MAX_LOG_ENTRY_BYTES {} — \
                 reduce pipeline state or shorten batches",
                buf.len(),
                MAX_LOG_ENTRY_BYTES
            )));
        }

        let schema_id = data.schemas().fingerprint();
        self.store
            .save_checkpoint(*claim_id, stage_idx, buf, schema_id)
            .await
    }

    fn now_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::Dataset;
    // Only the windowed chain-carry test below reads the accumulator
    // sentinel, so without `windows` the import has no user either.
    #[cfg(feature = "windows")]
    use crate::distributed::checkpoint::ACCUMULATOR_STAGE_SENTINEL;
    use crate::distributed::checkpoint::{
        Checkpoint, CheckpointStore, PROCESSOR_STATE_STAGE_SENTINEL,
    };
    use crate::distributed::partition::{BatchClaim, PartitionSource};
    use crate::system::{SystemMeta, system_fn};
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    /// Lease TTL [`MemoryStore`] grants, long enough that no test lease expires
    /// on its own.
    const TEST_LEASE_TTL_MILLIS: u64 = 30_000;

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
        /// Registration order is claim order.
        batches: Vec<BatchState>,
        checkpoints: HashMap<(Uuid, u32), Checkpoint>,
    }

    /// In-memory shared store: real claim bookkeeping, no I/O.
    ///
    /// The runner builds every dataset from its own `world_factory`, so the
    /// fixture keeps no master-batch payload, only the claim lifecycle and the
    /// checkpoints the runner writes.
    ///
    /// Cloning shares one set of batches, so two runners (or a runner plus the
    /// assertions) can hold the same store.
    #[derive(Clone, Default)]
    struct MemoryStore {
        inner: Arc<Mutex<Inner>>,
    }

    impl MemoryStore {
        fn new() -> Self {
            Self::default()
        }

        /// Register a batch whose whole row range becomes claimable.
        fn register(&self, batch_id: u64, total_rows: u32) {
            self.lock().batches.push(BatchState {
                batch_id,
                component: "test".to_string(),
                schema_id: 1,
                total_rows,
                status: RangeStatus::Pending,
                claim_id: Uuid::nil(),
                instance_id: Uuid::nil(),
                lease_expires_at: 0,
            });
        }

        /// Every checkpoint stored at `stage_idx`, in unspecified order.
        fn checkpoints_at(&self, stage_idx: u32) -> Vec<Checkpoint> {
            self.lock()
                .checkpoints
                .iter()
                .filter(|((_, s), _)| *s == stage_idx)
                .map(|(_, c)| c.clone())
                .collect()
        }

        fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
            self.inner.lock().unwrap_or_else(|e| e.into_inner())
        }

        fn resolve(
            inner: &mut Inner,
            claim_id: Uuid,
            instance_id: Uuid,
        ) -> SaciResult<&mut BatchState> {
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
    }

    #[async_trait]
    impl PartitionSource for MemoryStore {
        async fn claim_next_batch(&self, instance_id: Uuid) -> SaciResult<Option<BatchClaim>> {
            let now = now_millis();
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
            batch.lease_expires_at = now + TEST_LEASE_TTL_MILLIS;
            Ok(Some(BatchClaim {
                batch_id: batch.batch_id,
                component: batch.component.clone(),
                row_range: 0..batch.total_rows,
                schema_id: batch.schema_id,
                claim_id,
                instance_id,
                lease_expires_at: batch.lease_expires_at,
                lease_ttl_millis: TEST_LEASE_TTL_MILLIS,
                claimed_at: Instant::now(),
            }))
        }

        async fn renew_claim(&self, claim_id: Uuid, instance_id: Uuid) -> SaciResult<u64> {
            let expires_at = now_millis() + TEST_LEASE_TTL_MILLIS;
            let mut inner = self.lock();
            MemoryStore::resolve(&mut inner, claim_id, instance_id)?.lease_expires_at = expires_at;
            Ok(expires_at)
        }

        async fn ack_claim(&self, claim_id: Uuid, instance_id: Uuid) -> SaciResult<()> {
            let mut inner = self.lock();
            let batch = MemoryStore::resolve(&mut inner, claim_id, instance_id)?;
            batch.status = RangeStatus::Completed;
            batch.lease_expires_at = 0;
            Ok(())
        }

        async fn release_claim(&self, claim_id: Uuid, instance_id: Uuid) -> SaciResult<()> {
            let mut inner = self.lock();
            let batch = MemoryStore::resolve(&mut inner, claim_id, instance_id)?;
            batch.status = RangeStatus::Pending;
            batch.claim_id = Uuid::nil();
            batch.instance_id = Uuid::nil();
            batch.lease_expires_at = 0;
            Ok(())
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
            Ok(())
        }

        async fn load_checkpoint(
            &self,
            claim_id: Uuid,
            stage_idx: u32,
        ) -> SaciResult<Option<Checkpoint>> {
            Ok(self.lock().checkpoints.get(&(claim_id, stage_idx)).cloned())
        }
    }

    fn now_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    fn empty_data() -> Dataset {
        Dataset::new()
    }

    #[tokio::test]
    async fn test_runner_happy_path_no_batches() {
        let store = MemoryStore::new();
        let pipeline = Pipeline::new("test");
        let config = RunnerConfig {
            max_batches: Some(5),
            checkpoint_strategy: CheckpointStrategy::None,
            ..Default::default()
        };
        let runner = DistributedRunner::new(store, Box::new(pipeline), config);
        let processed = runner.run(empty_data).await.unwrap();
        assert_eq!(processed, 0);
    }

    #[tokio::test]
    async fn test_runner_processes_one_batch() {
        use std::sync::atomic::AtomicU32;

        let store = MemoryStore::new();
        store.register(0, 10);

        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = Arc::clone(&counter);

        let mut pipeline = Pipeline::new("test");
        pipeline.add_system(system_fn(SystemMeta::new("increment"), move |_data| {
            counter_clone.fetch_add(1, AtomicOrdering::Relaxed);
            Ok(())
        }));

        let config = RunnerConfig {
            max_batches: Some(1),
            checkpoint_strategy: CheckpointStrategy::None,
            ..Default::default()
        };
        let runner = DistributedRunner::new(store, Box::new(pipeline), config);
        let processed = runner.run(empty_data).await.unwrap();
        assert_eq!(processed, 1);
        assert_eq!(counter.load(AtomicOrdering::Relaxed), 1);
    }

    /// A partition source that fails lease renewal to simulate expiry.
    struct ExpirableSource {
        inner: MemoryStore,
        fail_renewal: bool,
    }

    #[async_trait]
    impl PartitionSource for ExpirableSource {
        async fn claim_next_batch(&self, id: Uuid) -> SaciResult<Option<BatchClaim>> {
            self.inner.claim_next_batch(id).await
        }
        async fn renew_claim(&self, _claim_id: Uuid, _instance_id: Uuid) -> SaciResult<u64> {
            if self.fail_renewal {
                Err(SaciError::generic("simulated lease expiry"))
            } else {
                Ok(u64::MAX)
            }
        }
        async fn ack_claim(&self, claim_id: Uuid, instance_id: Uuid) -> SaciResult<()> {
            self.inner.ack_claim(claim_id, instance_id).await
        }
        async fn release_claim(&self, claim_id: Uuid, instance_id: Uuid) -> SaciResult<()> {
            self.inner.release_claim(claim_id, instance_id).await
        }
        fn should_renew(&self, _claim: &BatchClaim) -> bool {
            self.fail_renewal
        }
    }

    // The runner requires a store implementing both traits.
    #[async_trait]
    impl CheckpointStore for ExpirableSource {
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

    #[tokio::test]
    async fn test_runner_lease_expiry_causes_clean_exit() {
        let inner = MemoryStore::new();
        inner.register(0, 10);

        let source = ExpirableSource {
            inner,
            fail_renewal: true,
        };

        let mut pipeline = Pipeline::new("test");
        pipeline.add_system(system_fn(SystemMeta::new("noop"), |_data| Ok(())));

        let config = RunnerConfig {
            max_batches: Some(1),
            checkpoint_strategy: CheckpointStrategy::None,
            ..Default::default()
        };
        let runner = DistributedRunner::new(source, Box::new(pipeline), config);
        let result = runner.run(empty_data).await;

        assert!(
            result.is_err(),
            "expected lease expiry error, got {result:?}"
        );
    }

    /// Partition source that counts `release_claim` and `ack_claim` calls and delegates
    /// the rest to a real inner store.
    ///
    /// Only the first claim is issued: `release_claim` returns the batch to `Pending`, so
    /// the runner loop would otherwise re-find it on every iteration and cycle forever.
    struct CountingSource {
        inner: MemoryStore,
        release_count: Arc<AtomicUsize>,
        ack_count: Arc<AtomicUsize>,
        claims_issued: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl PartitionSource for CountingSource {
        async fn claim_next_batch(&self, id: Uuid) -> SaciResult<Option<BatchClaim>> {
            if self.claims_issued.load(AtomicOrdering::SeqCst) >= 1 {
                return Ok(None);
            }
            let result = self.inner.claim_next_batch(id).await?;
            if result.is_some() {
                self.claims_issued.fetch_add(1, AtomicOrdering::SeqCst);
            }
            Ok(result)
        }
        async fn renew_claim(&self, claim_id: Uuid, instance_id: Uuid) -> SaciResult<u64> {
            self.inner.renew_claim(claim_id, instance_id).await
        }
        async fn ack_claim(&self, claim_id: Uuid, instance_id: Uuid) -> SaciResult<()> {
            self.ack_count.fetch_add(1, AtomicOrdering::SeqCst);
            self.inner.ack_claim(claim_id, instance_id).await
        }
        async fn release_claim(&self, claim_id: Uuid, instance_id: Uuid) -> SaciResult<()> {
            self.release_count.fetch_add(1, AtomicOrdering::SeqCst);
            self.inner.release_claim(claim_id, instance_id).await
        }
        fn should_renew(&self, _claim: &BatchClaim) -> bool {
            false
        }
    }

    /// Checkpoint store that always fails `save_checkpoint`.
    #[async_trait]
    impl CheckpointStore for CountingSource {
        async fn save_checkpoint(
            &self,
            _claim_id: Uuid,
            _stage_idx: u32,
            _ipc_bytes: Vec<u8>,
            _schema_id: u32,
        ) -> SaciResult<()> {
            Err(SaciError::generic("simulated checkpoint failure"))
        }
        async fn load_checkpoint(
            &self,
            claim_id: Uuid,
            stage_idx: u32,
        ) -> SaciResult<Option<Checkpoint>> {
            self.inner.load_checkpoint(claim_id, stage_idx).await
        }
    }

    /// A failed `save_checkpoint` must release the claim for at-least-once retry, not ack.
    #[tokio::test]
    async fn test_checkpoint_failure_releases_not_acks() {
        let inner = MemoryStore::new();
        inner.register(0, 10);

        let release_count = Arc::new(AtomicUsize::new(0));
        let ack_count = Arc::new(AtomicUsize::new(0));
        let source = CountingSource {
            inner,
            release_count: Arc::clone(&release_count),
            ack_count: Arc::clone(&ack_count),
            claims_issued: Arc::new(AtomicUsize::new(0)),
        };

        let mut pipeline = Pipeline::new("test");
        pipeline.add_system(system_fn(SystemMeta::new("noop"), |_data| Ok(())));

        let config = RunnerConfig {
            max_batches: Some(1),
            // EveryStage forces a checkpoint on the run, which fails immediately.
            checkpoint_strategy: CheckpointStrategy::EveryStage,
            ..Default::default()
        };
        let runner = DistributedRunner::new(source, Box::new(pipeline), config);

        let processed = runner
            .run(empty_data)
            .await
            .expect("runner should skip failed batch without surfacing error");
        assert_eq!(
            processed, 0,
            "batch whose checkpoint failed must not be counted as processed"
        );

        assert_eq!(
            release_count.load(AtomicOrdering::SeqCst),
            1,
            "expected exactly one release on checkpoint failure"
        );
        assert_eq!(
            ack_count.load(AtomicOrdering::SeqCst),
            0,
            "must NOT ack a claim whose checkpoint failed"
        );
    }

    /// Two-run scenario: the first run creates accumulator rows, the second loads them.
    #[cfg(feature = "windows")]
    #[tokio::test]
    async fn test_accumulator_persists_across_two_runs() {
        use crate::component::Component as _;
        use crate::windows::accumulator::WindowAccumulator;
        use std::sync::atomic::AtomicU32;

        let store = MemoryStore::new();
        // Seed two batches so two runs happen.
        for batch_id in 0u64..2 {
            store.register(batch_id, 10);
        }

        // A system that appends one accumulator row per run.
        let run_count = Arc::new(AtomicU32::new(0));
        let run_count_clone = Arc::clone(&run_count);

        let mut pipeline = Pipeline::new("test");
        pipeline.add_system(system_fn(
            SystemMeta::new("append_accumulator"),
            move |data: &mut Dataset| {
                let run = run_count_clone.fetch_add(1, AtomicOrdering::Relaxed);
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
                    data.append::<WindowAccumulator>(&[row]).unwrap();
                }
                Ok(())
            },
        ));

        let world_factory = || {
            let mut d = Dataset::new();
            d.register_component::<WindowAccumulator>().unwrap();
            d
        };

        let config = RunnerConfig {
            max_batches: Some(2),
            checkpoint_strategy: CheckpointStrategy::None,
            ..Default::default()
        };
        let runner = DistributedRunner::new(store, Box::new(pipeline), config);
        let processed = runner.run(world_factory).await.unwrap();
        assert_eq!(processed, 2);
        assert_eq!(run_count.load(AtomicOrdering::Relaxed), 2);
    }

    /// Chain-carry proof: the second claim must load the accumulator rows the
    /// first claim wrote (load under the *prior* claim id, not the fresh one),
    /// and both claims must leave their own checkpoint in the store.
    #[cfg(feature = "windows")]
    #[tokio::test]
    async fn test_accumulator_values_carry_across_claims() {
        use crate::component::Component as _;
        use crate::windows::accumulator::WindowAccumulator;
        use std::sync::atomic::AtomicU32;

        let store = MemoryStore::new();
        // Two batches → two claims, one per claim pass. Each claim pass is a
        // separate world, so only the chained accumulator can carry rows
        // from the first claim to the second.
        for batch_id in 0u64..2 {
            store.register(batch_id, 10);
        }

        // The system records how many accumulator rows the dataset held at
        // entry (the restored prior state) and appends one more, with a
        // distinct window_id per run.
        let run_count = Arc::new(AtomicU32::new(0));
        let entry_rows = Arc::new(Mutex::new(Vec::<u64>::new()));
        let run_count_clone = Arc::clone(&run_count);
        let entry_rows_clone = Arc::clone(&entry_rows);

        let mut pipeline = Pipeline::new("test");
        pipeline.add_system(system_fn(
            SystemMeta::new("append_accumulator"),
            move |data: &mut Dataset| {
                entry_rows_clone.lock().unwrap().push(data.rows() as u64);
                let run = run_count_clone.fetch_add(1, AtomicOrdering::Relaxed);
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
                    data.append::<WindowAccumulator>(&[row]).unwrap();
                }
                Ok(())
            },
        ));

        let world_factory = || {
            let mut d = Dataset::new();
            d.register_component::<WindowAccumulator>().unwrap();
            d
        };

        let config = RunnerConfig {
            max_batches: Some(2),
            checkpoint_strategy: CheckpointStrategy::None,
            ..Default::default()
        };
        let runner = DistributedRunner::new(store.clone(), Box::new(pipeline), config);
        let processed = runner.run(world_factory).await.unwrap();
        assert_eq!(processed, 2);
        assert_eq!(run_count.load(AtomicOrdering::Relaxed), 2);
        assert_eq!(
            entry_rows.lock().unwrap().as_slice(),
            &[0, 1],
            "the second claim must start from the first claim's accumulator rows"
        );

        // Store-level: both claims left an accumulator checkpoint, the second
        // carrying the first's row plus its own (1 row and 2 rows).
        let mut checkpoint_row_counts: Vec<u64> = store
            .checkpoints_at(ACCUMULATOR_STAGE_SENTINEL)
            .into_iter()
            .filter_map(|c| {
                let dataset = Dataset::read_ipc(&mut c.payload.as_slice())
                    .expect("checkpoint payload is single-component IPC");
                dataset
                    .batch_for(WindowAccumulator::name())
                    .map(|b| b.num_rows() as u64)
            })
            .collect();
        checkpoint_row_counts.sort_unstable();
        assert_eq!(
            checkpoint_row_counts,
            vec![1, 2],
            "claim 1 persists 1 row; claim 2 persists claim 1's row plus its own"
        );
    }

    /// A failed accumulator save must release the batch instead of acking it, so replay
    /// stays possible.
    #[cfg(feature = "windows")]
    #[tokio::test]
    async fn test_accumulator_save_failure_releases_not_acks() {
        use crate::windows::accumulator::WindowAccumulator;

        let inner = MemoryStore::new();
        inner.register(0, 10);

        let release_count = Arc::new(AtomicUsize::new(0));
        let ack_count = Arc::new(AtomicUsize::new(0));
        let claims_issued = Arc::new(AtomicUsize::new(0));

        struct FailingAccumStore {
            inner: MemoryStore,
            release_count: Arc<AtomicUsize>,
            ack_count: Arc<AtomicUsize>,
            claims_issued: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl PartitionSource for FailingAccumStore {
            async fn claim_next_batch(&self, id: Uuid) -> SaciResult<Option<BatchClaim>> {
                if self.claims_issued.load(AtomicOrdering::SeqCst) >= 1 {
                    return Ok(None);
                }
                let result = self.inner.claim_next_batch(id).await?;
                if result.is_some() {
                    self.claims_issued.fetch_add(1, AtomicOrdering::SeqCst);
                }
                Ok(result)
            }
            async fn renew_claim(&self, id: Uuid, instance_id: Uuid) -> SaciResult<u64> {
                self.inner.renew_claim(id, instance_id).await
            }
            async fn ack_claim(&self, id: Uuid, instance_id: Uuid) -> SaciResult<()> {
                self.ack_count.fetch_add(1, AtomicOrdering::SeqCst);
                self.inner.ack_claim(id, instance_id).await
            }
            async fn release_claim(&self, id: Uuid, instance_id: Uuid) -> SaciResult<()> {
                self.release_count.fetch_add(1, AtomicOrdering::SeqCst);
                self.inner.release_claim(id, instance_id).await
            }
            fn should_renew(&self, _: &BatchClaim) -> bool {
                false
            }
        }

        #[async_trait]
        impl CheckpointStore for FailingAccumStore {
            async fn save_checkpoint(&self, _: Uuid, _: u32, _: Vec<u8>, _: u32) -> SaciResult<()> {
                Err(SaciError::generic("simulated accumulator save failure"))
            }
            async fn load_checkpoint(
                &self,
                claim_id: Uuid,
                stage_idx: u32,
            ) -> SaciResult<Option<Checkpoint>> {
                self.inner.load_checkpoint(claim_id, stage_idx).await
            }
        }

        let source = FailingAccumStore {
            inner,
            release_count: Arc::clone(&release_count),
            ack_count: Arc::clone(&ack_count),
            claims_issued: Arc::clone(&claims_issued),
        };

        let mut pipeline = Pipeline::new("test");
        pipeline.add_system(system_fn(SystemMeta::new("noop"), |_data| Ok(())));

        let world_factory = || {
            let mut d = Dataset::new();
            d.register_component::<WindowAccumulator>().unwrap();
            d
        };

        let config = RunnerConfig {
            max_batches: Some(1),
            checkpoint_strategy: CheckpointStrategy::None,
            ..Default::default()
        };
        let runner = DistributedRunner::new(source, Box::new(pipeline), config);

        let processed = runner
            .run(world_factory)
            .await
            .expect("runner should skip failed accumulator save without surfacing error");

        assert_eq!(
            processed, 0,
            "batch with failed accumulator save must not be counted"
        );
        assert_eq!(
            release_count.load(AtomicOrdering::SeqCst),
            1,
            "expected exactly one release on accumulator save failure"
        );
        assert_eq!(
            ack_count.load(AtomicOrdering::SeqCst),
            0,
            "must NOT ack a claim whose accumulator save failed"
        );
    }

    /// The runtime's opaque state blob is parked under its own sentinel and
    /// reloaded on the next claim, independently of the accumulator.
    #[tokio::test]
    async fn test_processor_state_parked_under_its_own_sentinel() {
        let store = MemoryStore::new();
        store.register(0, 10);

        let mut pipeline = Pipeline::new("test");
        pipeline.add_system(system_fn(SystemMeta::new("noop"), |_data| Ok(())));

        let config = RunnerConfig {
            max_batches: Some(1),
            checkpoint_strategy: CheckpointStrategy::EveryStage,
            ..Default::default()
        };
        let runner = DistributedRunner::new(store.clone(), Box::new(pipeline), config);
        assert_eq!(runner.run(empty_data).await.unwrap(), 1);

        // A native pipeline returns no state blob, so the sentinel stays empty
        // while the data stage checkpoint is written.
        assert!(
            store
                .checkpoints_at(PROCESSOR_STATE_STAGE_SENTINEL)
                .is_empty(),
            "a native pipeline carries no runtime state blob"
        );
        assert_eq!(
            store.checkpoints_at(0).len(),
            1,
            "EveryStage must leave exactly one data-stage checkpoint"
        );
    }

    #[tokio::test]
    async fn test_shutdown_between_batches_clean() {
        let store = MemoryStore::new();
        // Seed two batches.
        for batch_id in 0u64..2 {
            store.register(batch_id, 10);
        }

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
            .run_with_shutdown(empty_data, shutdown)
            .await
            .unwrap();
        assert_eq!(processed, 0, "cancelled runner must process 0 batches");
    }
}
