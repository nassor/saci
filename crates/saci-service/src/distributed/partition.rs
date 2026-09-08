//! Arrow-IPC-native partition source traits and claim types.
//!
//! [`PartitionSource`] claims slices (row ranges) of a replicated master
//! `RecordBatch`. Each claim is uniquely identified by a `Uuid` and has a
//! lease that must be renewed before it expires.

use std::ops::Range;
use std::time::Instant;

use async_trait::async_trait;
use uuid::Uuid;

use crate::SaciResult;

/// Hard cap on the Arrow IPC bytes of a single persisted payload: the master
/// batch a partition source registers, and one stage checkpoint, since this
/// value is the
/// [`CheckpointStore::max_checkpoint_bytes`](crate::distributed::checkpoint::CheckpointStore::max_checkpoint_bytes)
/// default.
///
/// Payloads above this limit are rejected before they are written; consumers
/// that produce larger batches must split them first. Both halves travel
/// inside a raft log entry in cluster mode, which is where the limit comes
/// from.
pub const MAX_LOG_ENTRY_BYTES: usize = 1024 * 1024; // 1 MiB

/// A granted claim on a row-range slice of a replicated master `RecordBatch`.
///
/// The runner holds this while processing. It must renew the lease before
/// `lease_expires_at` or the batch may be reclaimed by another instance.
///
/// ## Invariants
///
/// - `row_range.start < row_range.end <= master_batch.total_rows`.
/// - `lease_expires_at` is unix milliseconds; compare with
///   `SystemTime::now()` in millis.
/// - `claimed_at` is a monotonic [`Instant`] stamped at claim-grant time on
///   the local runner. Use it for renewal-threshold decisions instead of
///   wall-clock comparisons to avoid NTP skew.
#[derive(Debug, Clone)]
pub struct BatchClaim {
    /// Stable identifier for the master batch this claim belongs to.
    pub batch_id: u64,
    /// Name of the Arrow component in the [`Dataset`](crate::Dataset).
    pub component: String,
    /// Half-open row-range `[start, end)` within the master batch.
    pub row_range: Range<u32>,
    /// Schema version identifier for the master batch.
    ///
    /// Followers should reject or upgrade if they run an older binary.
    pub schema_id: u32,
    /// Unique identifier for this specific claim grant.
    pub claim_id: Uuid,
    /// The runner instance that holds this claim.
    pub instance_id: Uuid,
    /// Lease expiry time in unix milliseconds (wall-clock).
    ///
    /// The state machine uses this for deterministic expiry decisions.
    /// Local runner code should prefer `claimed_at` + `lease_ttl_millis`
    /// for renewal checks to avoid NTP skew.
    pub lease_expires_at: u64,
    /// Lease TTL in milliseconds as granted by the state machine.
    ///
    /// Used to compute the renewal threshold without wall-clock dependency.
    pub lease_ttl_millis: u64,
    /// Monotonic instant when this claim was received by the local runner.
    ///
    /// Not serialized; stamped at claim-grant time on the local process.
    /// Use `claimed_at.elapsed()` for renewal decisions.
    pub claimed_at: Instant,
}

/// Source of Arrow-columnar work batches for distributed processing.
///
/// Implementations coordinate across instances to ensure at-most-one claim
/// per row range at any given time. [`RedbSharedStore`](crate::distributed::RedbSharedStore)
/// does it by proposing the claim through raft, where the state machine
/// rejects a range that overlaps a live one.
///
/// ## Lease contract
///
/// A runner MUST stop processing immediately if [`renew_claim`](Self::renew_claim)
/// returns an error. Continuing after a lease failure breaks the at-most-once
/// processing guarantee and can corrupt downstream state.
///
/// ## Example
///
/// ```no_run
/// # #[cfg(feature = "distributed")]
/// # {
/// use saci_service::distributed::partition::PartitionSource;
///
/// async fn process_all(source: &impl PartitionSource) -> saci_service::SaciResult<()> {
///     let instance = uuid::Uuid::now_v7();
///     while let Some(claim) = source.claim_next_batch(instance).await? {
///         // … process …
///         source.ack_claim(claim.claim_id, claim.instance_id).await?;
///     }
///     Ok(())
/// }
/// # }
/// ```
#[async_trait]
pub trait PartitionSource: Send + Sync {
    /// Claim the next available row-range batch, if any.
    ///
    /// Returns `Ok(None)` when no pending batches are available.
    async fn claim_next_batch(&self, instance_id: Uuid) -> SaciResult<Option<BatchClaim>>;

    /// Renew the lease on an existing claim.
    ///
    /// # Critical behaviour
    ///
    /// The runner MUST stop processing if this returns an error.
    async fn renew_claim(&self, claim_id: Uuid, instance_id: Uuid) -> SaciResult<u64>;

    /// Acknowledge that processing completed successfully.
    ///
    /// After a successful ack the row range is marked completed and will not
    /// be reclaimed.
    async fn ack_claim(&self, claim_id: Uuid, instance_id: Uuid) -> SaciResult<()>;

    /// Release a claim back to the pending pool for retry.
    ///
    /// Called when the runner decides it cannot complete the batch (e.g. after
    /// a fatal system error that is not retried).
    async fn release_claim(&self, claim_id: Uuid, instance_id: Uuid) -> SaciResult<()>;

    /// Query whether the runner should attempt to renew its lease now.
    ///
    /// Returns `true` when less than 30 % of the lease TTL (or 10 s, whichever
    /// is smaller) remains. The default impl uses `claim.claimed_at.elapsed()`
    /// against `claim.lease_ttl_millis` so the decision is immune to NTP steps.
    fn should_renew(&self, claim: &BatchClaim) -> bool {
        let elapsed_ms = claim.claimed_at.elapsed().as_millis() as u64;
        let remaining_ms = claim.lease_ttl_millis.saturating_sub(elapsed_ms);
        let threshold = (claim.lease_ttl_millis * 3 / 10).min(10_000);
        remaining_ms < threshold
    }

    /// Sweep expired leases: reset any `Claimed` row-range whose lease has
    /// expired before `now_millis` back to `Pending` so another runner can
    /// reclaim it.
    ///
    /// Returns the number of claims freed. The default implementation is a
    /// no-op (returns `Ok(0)`) for sources that do not support expiry sweeps.
    /// [`RedbSharedStore`](crate::distributed::RedbSharedStore) overrides it
    /// with `ConsensusCommand::ReclaimExpired`.
    async fn reclaim_expired(&self, _now_millis: u64) -> SaciResult<u32> {
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// Build a claim with `claimed_at` set so that `elapsed()` is approximately
    /// `elapsed_ms` milliseconds when the test runs.
    fn make_claim_elapsed(elapsed_ms: u64, lease_ttl_millis: u64) -> BatchClaim {
        BatchClaim {
            batch_id: 1,
            component: "orders".to_string(),
            row_range: 0..100,
            schema_id: 1,
            claim_id: Uuid::now_v7(),
            instance_id: Uuid::now_v7(),
            lease_expires_at: 0, // unused by default impl
            lease_ttl_millis,
            claimed_at: Instant::now() - Duration::from_millis(elapsed_ms),
        }
    }

    struct DummySource;

    #[async_trait]
    impl PartitionSource for DummySource {
        async fn claim_next_batch(&self, _: Uuid) -> SaciResult<Option<BatchClaim>> {
            Ok(None)
        }
        async fn renew_claim(&self, _: Uuid, _: Uuid) -> SaciResult<u64> {
            Ok(0)
        }
        async fn ack_claim(&self, _: Uuid, _: Uuid) -> SaciResult<()> {
            Ok(())
        }
        async fn release_claim(&self, _: Uuid, _: Uuid) -> SaciResult<()> {
            Ok(())
        }
    }

    #[test]
    fn test_should_renew_when_expiry_near() {
        let src = DummySource;
        // TTL=90s, 89s elapsed → 1s remaining → threshold=10s → renew.
        assert!(src.should_renew(&make_claim_elapsed(89_000, 90_000)));
        // TTL=90s, 1s elapsed → 89s remaining → well above 10s threshold → no renew.
        assert!(!src.should_renew(&make_claim_elapsed(1_000, 90_000)));
    }

    /// The `should_renew` threshold formula across several TTLs.
    #[test]
    fn test_should_renew_math_fixed() {
        let src = DummySource;

        // TTL=10s → threshold=3s. 8s elapsed → 2s remaining: renew.
        assert!(src.should_renew(&make_claim_elapsed(8_000, 10_000)));
        // TTL=10s, 6s elapsed → 4s remaining → above 3s threshold: no renew.
        assert!(!src.should_renew(&make_claim_elapsed(6_000, 10_000)));

        // TTL=30s → threshold=9s. 22s elapsed → 8s remaining: renew.
        assert!(src.should_renew(&make_claim_elapsed(22_000, 30_000)));
        assert!(!src.should_renew(&make_claim_elapsed(20_000, 30_000)));

        // TTL=90s → threshold=10s (capped). 81s elapsed → 9s remaining: renew.
        assert!(src.should_renew(&make_claim_elapsed(81_000, 90_000)));
        assert!(!src.should_renew(&make_claim_elapsed(79_000, 90_000)));

        // TTL=300s → threshold=10s (capped). 291s elapsed → 9s remaining: renew.
        assert!(src.should_renew(&make_claim_elapsed(291_000, 300_000)));
        assert!(!src.should_renew(&make_claim_elapsed(289_000, 300_000)));
    }

    /// Just-issued claim must not trigger renewal.
    #[test]
    fn test_renew_not_triggered_when_plenty_remaining() {
        let src = DummySource;
        assert!(!src.should_renew(&make_claim_elapsed(1_000, 90_000)));
    }

    /// Just inside the threshold triggers renewal; at the boundary does not.
    #[test]
    fn test_renew_triggered_at_threshold() {
        let src = DummySource;
        assert!(src.should_renew(&make_claim_elapsed(80_001, 90_000)));
        assert!(!src.should_renew(&make_claim_elapsed(80_000, 90_000)));
    }

    #[test]
    fn test_arrow_batch_claim_clone() {
        let id = Uuid::now_v7();
        let inst = Uuid::now_v7();
        let a = BatchClaim {
            batch_id: 1,
            component: "test".to_string(),
            row_range: 0..50,
            schema_id: 2,
            claim_id: id,
            instance_id: inst,
            lease_expires_at: 9999,
            lease_ttl_millis: 90_000,
            claimed_at: Instant::now(),
        };
        let b = a.clone();
        assert_eq!(a.batch_id, b.batch_id);
        assert_eq!(a.claim_id, b.claim_id);
        assert_eq!(a.lease_ttl_millis, b.lease_ttl_millis);
    }

    #[test]
    fn test_max_log_entry_bytes_is_one_mib() {
        assert_eq!(MAX_LOG_ENTRY_BYTES, 1_048_576);
    }
}
