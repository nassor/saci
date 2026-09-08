//! openraft type configuration and consensus command/response types for SACI's
//! Arrow-IPC distributed consensus layer.
//!
//! ## Log entry size contract
//!
//! `ipc_bytes` fields in [`ConsensusCommand`] variants enforce a hard 1 MiB cap.
//! Producers must split larger batches before proposing. The check happens at the
//! [`RedbSharedStore`](super::store::RedbSharedStore) propose boundary, so entries
//! larger than `MAX_LOG_ENTRY_BYTES` never reach Raft.
//!
//! ## Serialization
//!
//! [`ConsensusCommand`] and [`ConsensusResponse`] are the openraft `D` and `R` types
//! for [`SaciTypeConfig`]: openraft presents them directly to `RaftLogStorage` /
//! `RaftStateMachine` without an intermediate string encoding. Persistence uses
//! `postcard` (see `storage.rs`), which is canonical by construction: stable byte
//! output for equal inputs, no JSON map-ordering ambiguity, no UTF-8 encoding cost.
//! Raft determinism depends on it, because two replicas applying the same committed
//! entry must produce byte-identical state.
//!
//! ## Schema evolution
//!
//! Every log entry that touches Arrow data carries a `schema_id` (u32). Followers
//! running an older binary must reject or upgrade entries they cannot understand.

use std::ops::Range;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(feature = "distributed-raft")]
openraft::declare_raft_types!(
    /// openraft type configuration parameterised for SACI's Arrow-IPC consensus.
    ///
    /// `D = ConsensusCommand` and `R = ConsensusResponse` let openraft carry the
    /// application types directly, with no intermediate JSON string encoding on the
    /// `client_write` path or the apply path. Log-entry persistence uses `postcard`
    /// (see `storage.rs`) for canonical, compact binary encoding.
    pub SaciTypeConfig:
        D = ConsensusCommand,
        R = ConsensusResponse,
        NodeId = u64,
        Node = openraft::BasicNode,
);

/// A command that can be committed through Raft and applied to the state machine.
///
/// Persistence uses `postcard` (see `storage.rs`). Arrow IPC payloads
/// (`ipc_bytes`) are bounded at
/// [`MAX_LOG_ENTRY_BYTES`](crate::distributed::partition::MAX_LOG_ENTRY_BYTES).
///
/// ## Determinism: `now_at_propose`
///
/// `RegisterMasterBatch`, `ClaimRowRange`, `RenewClaim`, and `Checkpoint` carry a
/// `now_at_propose: u64` unix-millis field populated by the **leader** at propose
/// time. Apply handlers never read `SystemTime`, so followers replaying the same
/// committed entry compute byte-identical state. The field is `#[serde(default)]`, so
/// postcard-encoded entries that omit it still decode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsensusCommand {
    /// Register a new master RecordBatch in the replicated state.
    ///
    /// `ipc_bytes` must be <1 MiB or the command is rejected before proposing.
    RegisterMasterBatch {
        batch_id: u64,
        component: String,
        schema_id: u32,
        /// Arrow IPC bytes; must be <1 MiB.
        #[serde(with = "serde_bytes")]
        ipc_bytes: Vec<u8>,
        total_rows: u32,
        /// Unix millis stamped by the leader at propose time. Used for
        /// `MasterBatchRecord::created_at` so apply is deterministic.
        #[serde(default)]
        now_at_propose: u64,
    },
    /// Claim a row-range of a master batch for exclusive processing.
    ClaimRowRange {
        batch_id: u64,
        row_range_start: u32,
        row_range_end: u32,
        claim_id: Uuid,
        instance_id: Uuid,
        lease_ttl_millis: u64,
        /// Unix millis stamped by the leader at propose time. `lease_expires_at`
        /// is computed as `now_at_propose + lease_ttl_millis` inside apply.
        #[serde(default)]
        now_at_propose: u64,
    },
    /// Renew an existing claim's lease.
    RenewClaim {
        claim_id: Uuid,
        instance_id: Uuid,
        lease_ttl_millis: u64,
        /// Unix millis stamped by the leader at propose time. New expiry is
        /// `now_at_propose + lease_ttl_millis`.
        #[serde(default)]
        now_at_propose: u64,
    },
    /// Acknowledge successful processing.
    AckClaim { claim_id: Uuid, instance_id: Uuid },
    /// Release a claim back to the pending pool.
    ReleaseClaim { claim_id: Uuid, instance_id: Uuid },
    /// Write a checkpoint for a claim at a pipeline stage.
    ///
    /// `ipc_bytes` must be <1 MiB or the command is rejected before proposing.
    Checkpoint {
        claim_id: Uuid,
        stage_idx: u32,
        /// Arrow IPC bytes; must be <1 MiB.
        #[serde(with = "serde_bytes")]
        ipc_bytes: Vec<u8>,
        schema_id: u32,
        /// Unix millis stamped by the leader at propose time. Used for
        /// `CheckpointRecord::created_at`.
        #[serde(default)]
        now_at_propose: u64,
    },
    /// Record a liveness heartbeat for an instance.
    Heartbeat { instance_id: Uuid, at: u64 },
    /// Sweep expired leases: flip `Claimed → Pending` for any claim whose
    /// `lease_expires_at < now_at_propose`. Proposed periodically by runners.
    ReclaimExpired {
        /// Unix millis at propose time. Claims expiring before this are freed.
        now_at_propose: u64,
    },
    /// Permanently disqualify a master batch.
    ///
    /// Proposed by a runner when a batch's `release_attempts` counter has
    /// exceeded `RunnerConfig::max_claim_releases`. The state machine marks
    /// the batch `Poisoned`, stamps `poisoned_at`, and removes it from the
    /// `PENDING_BATCHES` secondary index so `claim_next_batch` never returns
    /// it again.
    ///
    /// Idempotent: a second `PoisonBatch` against an already-poisoned batch
    /// is a no-op and preserves the first-writer `poisoned_at` timestamp.
    PoisonBatch {
        batch_id: u64,
        /// Unix millis stamped by the leader at propose time. Used as
        /// `MasterBatchRecord::poisoned_at` so apply is deterministic.
        now_at_propose: u64,
    },
}

/// Return value from applying a [`ConsensusCommand`] to the state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsensusResponse {
    /// Master batch registered successfully.
    MasterBatchRegistered { batch_id: u64 },
    /// Row range claimed; carries the full claim.
    BatchClaimed {
        batch_id: u64,
        component: String,
        row_range_start: u32,
        row_range_end: u32,
        schema_id: u32,
        claim_id: Uuid,
        instance_id: Uuid,
        lease_expires_at: u64,
    },
    /// Claim lease renewed; carries new expiry (unix millis).
    ClaimRenewed { expires_at: u64 },
    /// Claim acknowledged successfully.
    ClaimAcked,
    /// Claim released back to pending pool.
    ClaimReleased,
    /// Checkpoint stored; carries an internal counter for the checkpoint.
    CheckpointWritten { checkpoint_id: u64 },
    /// Heartbeat recorded.
    HeartbeatRecorded,
    /// No pending batch was available to claim.
    NoBatchAvailable,
    /// Expired leases reclaimed; carries count of claims freed.
    ExpiredReclaimed { reclaimed_count: u32 },
    /// Batch poisoned. Returned on both the first-writer
    /// transition and the idempotent no-op path; carries the
    /// first-writer `poisoned_at` timestamp so callers can log a stable
    /// value even on a raced re-propose.
    BatchPoisoned { batch_id: u64, poisoned_at: u64 },
    /// Application-level error (recoverable; not a fatal Raft error).
    Error { message: String },
}

/// Lifecycle state of a row-range claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClaimStatus {
    Pending,
    Claimed,
    Completed,
}

impl ConsensusCommand {
    /// Return the Arrow IPC bytes slice if this command carries one.
    pub fn ipc_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::RegisterMasterBatch { ipc_bytes, .. } | Self::Checkpoint { ipc_bytes, .. } => {
                Some(ipc_bytes)
            }
            _ => None,
        }
    }
}

/// Reconstruct the row range from flat start/end fields.
pub fn row_range(start: u32, end: u32) -> Range<u32> {
    start..end
}

impl std::fmt::Display for ConsensusCommand {
    /// Concise, non-allocating summary suitable for Raft log tracing, required by the
    /// openraft `AppData` bound.
    ///
    /// Omits `ipc_bytes` payloads: they reach 1 MiB and must not end up in log or
    /// trace output. The variant tag plus identifying keys is enough for operational
    /// visibility.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RegisterMasterBatch {
                batch_id,
                component,
                schema_id,
                total_rows,
                ..
            } => write!(
                f,
                "RegisterMasterBatch(batch={batch_id}, comp={component}, schema={schema_id}, rows={total_rows})"
            ),
            Self::ClaimRowRange {
                batch_id,
                row_range_start,
                row_range_end,
                claim_id,
                ..
            } => write!(
                f,
                "ClaimRowRange(batch={batch_id}, rows={row_range_start}..{row_range_end}, claim={claim_id})"
            ),
            Self::RenewClaim {
                claim_id,
                lease_ttl_millis,
                ..
            } => write!(f, "RenewClaim(claim={claim_id}, ttl_ms={lease_ttl_millis})"),
            Self::AckClaim { claim_id, .. } => write!(f, "AckClaim(claim={claim_id})"),
            Self::ReleaseClaim { claim_id, .. } => write!(f, "ReleaseClaim(claim={claim_id})"),
            Self::Checkpoint {
                claim_id,
                stage_idx,
                schema_id,
                ..
            } => write!(
                f,
                "Checkpoint(claim={claim_id}, stage={stage_idx}, schema={schema_id})"
            ),
            Self::Heartbeat { instance_id, at } => {
                write!(f, "Heartbeat(instance={instance_id}, at={at})")
            }
            Self::ReclaimExpired { now_at_propose } => {
                write!(f, "ReclaimExpired(now={now_at_propose})")
            }
            Self::PoisonBatch {
                batch_id,
                now_at_propose,
            } => write!(f, "PoisonBatch(batch={batch_id}, now={now_at_propose})"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── postcard round-trip: every ConsensusCommand variant ──────────────────
    // postcard is a [dev-dependencies] entry; use it by crate path.

    #[test]
    fn postcard_round_trip_register_master_batch() {
        let cmd = ConsensusCommand::RegisterMasterBatch {
            batch_id: 7,
            component: "orders".to_string(),
            schema_id: 3,
            ipc_bytes: vec![0xDE, 0xAD, 0xBE, 0xEF],
            total_rows: 500,
            now_at_propose: 12345,
        };
        let enc = postcard::to_allocvec(&cmd).unwrap();
        let dec: ConsensusCommand = postcard::from_bytes(&enc).unwrap();
        match dec {
            ConsensusCommand::RegisterMasterBatch {
                batch_id,
                schema_id,
                ipc_bytes,
                total_rows,
                now_at_propose,
                ..
            } => {
                assert_eq!(batch_id, 7);
                assert_eq!(schema_id, 3);
                assert_eq!(ipc_bytes, vec![0xDE, 0xAD, 0xBE, 0xEF]);
                assert_eq!(total_rows, 500);
                assert_eq!(now_at_propose, 12345);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn postcard_round_trip_claim_row_range() {
        let claim_id = Uuid::now_v7();
        let instance_id = Uuid::now_v7();
        let cmd = ConsensusCommand::ClaimRowRange {
            batch_id: 42,
            row_range_start: 0,
            row_range_end: 100,
            claim_id,
            instance_id,
            lease_ttl_millis: 30_000,
            now_at_propose: 999,
        };
        let enc = postcard::to_allocvec(&cmd).unwrap();
        let dec: ConsensusCommand = postcard::from_bytes(&enc).unwrap();
        match dec {
            ConsensusCommand::ClaimRowRange {
                batch_id,
                row_range_start,
                row_range_end,
                claim_id: cid,
                instance_id: iid,
                lease_ttl_millis,
                now_at_propose,
            } => {
                assert_eq!(batch_id, 42);
                assert_eq!(row_range_start, 0);
                assert_eq!(row_range_end, 100);
                assert_eq!(cid, claim_id);
                assert_eq!(iid, instance_id);
                assert_eq!(lease_ttl_millis, 30_000);
                assert_eq!(now_at_propose, 999);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn postcard_round_trip_renew_claim() {
        let claim_id = Uuid::now_v7();
        let instance_id = Uuid::now_v7();
        let cmd = ConsensusCommand::RenewClaim {
            claim_id,
            instance_id,
            lease_ttl_millis: 60_000,
            now_at_propose: 100,
        };
        let enc = postcard::to_allocvec(&cmd).unwrap();
        let dec: ConsensusCommand = postcard::from_bytes(&enc).unwrap();
        match dec {
            ConsensusCommand::RenewClaim {
                claim_id: cid,
                instance_id: iid,
                lease_ttl_millis,
                now_at_propose,
            } => {
                assert_eq!(cid, claim_id);
                assert_eq!(iid, instance_id);
                assert_eq!(lease_ttl_millis, 60_000);
                assert_eq!(now_at_propose, 100);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn postcard_round_trip_ack_claim() {
        let claim_id = Uuid::now_v7();
        let instance_id = Uuid::now_v7();
        let cmd = ConsensusCommand::AckClaim {
            claim_id,
            instance_id,
        };
        let enc = postcard::to_allocvec(&cmd).unwrap();
        let dec: ConsensusCommand = postcard::from_bytes(&enc).unwrap();
        assert!(
            matches!(dec, ConsensusCommand::AckClaim { claim_id: cid, instance_id: iid } if cid == claim_id && iid == instance_id)
        );
    }

    #[test]
    fn postcard_round_trip_release_claim() {
        let claim_id = Uuid::now_v7();
        let instance_id = Uuid::now_v7();
        let cmd = ConsensusCommand::ReleaseClaim {
            claim_id,
            instance_id,
        };
        let enc = postcard::to_allocvec(&cmd).unwrap();
        let dec: ConsensusCommand = postcard::from_bytes(&enc).unwrap();
        assert!(
            matches!(dec, ConsensusCommand::ReleaseClaim { claim_id: cid, instance_id: iid } if cid == claim_id && iid == instance_id)
        );
    }

    #[test]
    fn postcard_round_trip_checkpoint() {
        let claim_id = Uuid::now_v7();
        let ipc = vec![1u8, 2, 3, 4];
        let cmd = ConsensusCommand::Checkpoint {
            claim_id,
            stage_idx: 5,
            ipc_bytes: ipc.clone(),
            schema_id: 9,
            now_at_propose: 77,
        };
        let enc = postcard::to_allocvec(&cmd).unwrap();
        let dec: ConsensusCommand = postcard::from_bytes(&enc).unwrap();
        match dec {
            ConsensusCommand::Checkpoint {
                claim_id: cid,
                stage_idx,
                ipc_bytes,
                schema_id,
                now_at_propose,
            } => {
                assert_eq!(cid, claim_id);
                assert_eq!(stage_idx, 5);
                assert_eq!(ipc_bytes, ipc);
                assert_eq!(schema_id, 9);
                assert_eq!(now_at_propose, 77);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn postcard_round_trip_heartbeat() {
        let instance_id = Uuid::now_v7();
        let cmd = ConsensusCommand::Heartbeat {
            instance_id,
            at: 54321,
        };
        let enc = postcard::to_allocvec(&cmd).unwrap();
        let dec: ConsensusCommand = postcard::from_bytes(&enc).unwrap();
        match dec {
            ConsensusCommand::Heartbeat {
                instance_id: iid,
                at,
            } => {
                assert_eq!(iid, instance_id);
                assert_eq!(at, 54321);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn postcard_round_trip_reclaim_expired() {
        let cmd = ConsensusCommand::ReclaimExpired {
            now_at_propose: 999_999,
        };
        let enc = postcard::to_allocvec(&cmd).unwrap();
        let dec: ConsensusCommand = postcard::from_bytes(&enc).unwrap();
        assert!(matches!(
            dec,
            ConsensusCommand::ReclaimExpired {
                now_at_propose: 999_999
            }
        ));
    }

    // ── postcard round-trip: every ConsensusResponse variant ─────────────────

    #[test]
    fn postcard_round_trip_response_master_batch_registered() {
        let r = ConsensusResponse::MasterBatchRegistered { batch_id: 1 };
        let enc = postcard::to_allocvec(&r).unwrap();
        let dec: ConsensusResponse = postcard::from_bytes(&enc).unwrap();
        assert!(matches!(
            dec,
            ConsensusResponse::MasterBatchRegistered { batch_id: 1 }
        ));
    }

    #[test]
    fn postcard_round_trip_response_batch_claimed() {
        let claim_id = Uuid::now_v7();
        let instance_id = Uuid::now_v7();
        let r = ConsensusResponse::BatchClaimed {
            batch_id: 5,
            component: "items".to_string(),
            row_range_start: 10,
            row_range_end: 20,
            schema_id: 2,
            claim_id,
            instance_id,
            lease_expires_at: 99999,
        };
        let enc = postcard::to_allocvec(&r).unwrap();
        let dec: ConsensusResponse = postcard::from_bytes(&enc).unwrap();
        assert!(matches!(
            dec,
            ConsensusResponse::BatchClaimed { batch_id: 5, .. }
        ));
    }

    #[test]
    fn postcard_round_trip_response_claim_renewed() {
        let r = ConsensusResponse::ClaimRenewed { expires_at: 12345 };
        let enc = postcard::to_allocvec(&r).unwrap();
        let dec: ConsensusResponse = postcard::from_bytes(&enc).unwrap();
        assert!(matches!(
            dec,
            ConsensusResponse::ClaimRenewed { expires_at: 12345 }
        ));
    }

    #[test]
    fn postcard_round_trip_response_unit_variants() {
        for r in [
            ConsensusResponse::ClaimAcked,
            ConsensusResponse::ClaimReleased,
            ConsensusResponse::HeartbeatRecorded,
            ConsensusResponse::NoBatchAvailable,
        ] {
            let enc = postcard::to_allocvec(&r).unwrap();
            let dec: ConsensusResponse = postcard::from_bytes(&enc).unwrap();
            assert_eq!(format!("{r:?}"), format!("{dec:?}"));
        }
    }

    #[test]
    fn postcard_round_trip_response_checkpoint_written() {
        let r = ConsensusResponse::CheckpointWritten { checkpoint_id: 42 };
        let enc = postcard::to_allocvec(&r).unwrap();
        let dec: ConsensusResponse = postcard::from_bytes(&enc).unwrap();
        assert!(matches!(
            dec,
            ConsensusResponse::CheckpointWritten { checkpoint_id: 42 }
        ));
    }

    #[test]
    fn postcard_round_trip_response_expired_reclaimed() {
        let r = ConsensusResponse::ExpiredReclaimed { reclaimed_count: 7 };
        let enc = postcard::to_allocvec(&r).unwrap();
        let dec: ConsensusResponse = postcard::from_bytes(&enc).unwrap();
        assert!(matches!(
            dec,
            ConsensusResponse::ExpiredReclaimed { reclaimed_count: 7 }
        ));
    }

    #[test]
    fn postcard_round_trip_response_error() {
        let r = ConsensusResponse::Error {
            message: "something went wrong".to_string(),
        };
        let enc = postcard::to_allocvec(&r).unwrap();
        let dec: ConsensusResponse = postcard::from_bytes(&enc).unwrap();
        assert!(matches!(dec, ConsensusResponse::Error { .. }));
    }

    // ── malformed decode: verify Err, never panic ─────────────────────────────

    #[test]
    fn postcard_malformed_zero_len_returns_err() {
        let res: Result<ConsensusCommand, _> = postcard::from_bytes(&[]);
        assert!(res.is_err(), "zero-length decode must return Err");
        let res2: Result<ConsensusResponse, _> = postcard::from_bytes(&[]);
        assert!(res2.is_err(), "zero-length decode must return Err");
    }

    #[test]
    fn postcard_malformed_five_random_bytes_returns_err() {
        // These bytes do not encode any valid ConsensusCommand variant.
        let garbage = [0xFF, 0xFE, 0xFD, 0xFC, 0xFB];
        let res: Result<ConsensusCommand, _> = postcard::from_bytes(&garbage);
        assert!(res.is_err(), "garbage bytes must return Err for command");
        let res2: Result<ConsensusResponse, _> = postcard::from_bytes(&garbage);
        assert!(res2.is_err(), "garbage bytes must return Err for response");
    }

    #[test]
    fn postcard_malformed_large_garbage_returns_err() {
        // 2 KiB of 0xAB, not a valid encoding of any variant.
        let garbage = vec![0xAB_u8; 2048];
        let res: Result<ConsensusCommand, _> = postcard::from_bytes(&garbage);
        assert!(res.is_err(), "large garbage must return Err for command");
        let res2: Result<ConsensusResponse, _> = postcard::from_bytes(&garbage);
        assert!(res2.is_err(), "large garbage must return Err for response");
    }

    #[test]
    fn test_consensus_command_serde_round_trip_claim_row_range() {
        let claim_id = Uuid::now_v7();
        let instance_id = Uuid::now_v7();
        let cmd = ConsensusCommand::ClaimRowRange {
            batch_id: 42,
            row_range_start: 0,
            row_range_end: 100,
            claim_id,
            instance_id,
            lease_ttl_millis: 30_000,
            now_at_propose: 0,
        };
        let json = serde_json::to_string(&cmd).unwrap();
        let decoded: ConsensusCommand = serde_json::from_str(&json).unwrap();
        match decoded {
            ConsensusCommand::ClaimRowRange {
                batch_id,
                row_range_start,
                row_range_end,
                claim_id: cid,
                ..
            } => {
                assert_eq!(batch_id, 42);
                assert_eq!(row_range_start, 0);
                assert_eq!(row_range_end, 100);
                assert_eq!(cid, claim_id);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_consensus_command_serde_round_trip_register_master_batch() {
        let cmd = ConsensusCommand::RegisterMasterBatch {
            batch_id: 1,
            component: "orders".to_string(),
            schema_id: 7,
            ipc_bytes: vec![0xDE, 0xAD, 0xBE, 0xEF],
            total_rows: 1000,
            now_at_propose: 0,
        };
        let json = serde_json::to_string(&cmd).unwrap();
        let decoded: ConsensusCommand = serde_json::from_str(&json).unwrap();
        match decoded {
            ConsensusCommand::RegisterMasterBatch {
                batch_id,
                schema_id,
                ipc_bytes,
                total_rows,
                ..
            } => {
                assert_eq!(batch_id, 1);
                assert_eq!(schema_id, 7);
                assert_eq!(ipc_bytes, vec![0xDE, 0xAD, 0xBE, 0xEF]);
                assert_eq!(total_rows, 1000);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_consensus_command_serde_round_trip_checkpoint() {
        let claim_id = Uuid::now_v7();
        let ipc = vec![1u8, 2, 3, 4];
        let cmd = ConsensusCommand::Checkpoint {
            claim_id,
            stage_idx: 2,
            ipc_bytes: ipc.clone(),
            schema_id: 3,
            now_at_propose: 0,
        };
        let json = serde_json::to_string(&cmd).unwrap();
        let decoded: ConsensusCommand = serde_json::from_str(&json).unwrap();
        match decoded {
            ConsensusCommand::Checkpoint {
                claim_id: cid,
                stage_idx,
                ipc_bytes,
                schema_id,
                ..
            } => {
                assert_eq!(cid, claim_id);
                assert_eq!(stage_idx, 2);
                assert_eq!(ipc_bytes, ipc);
                assert_eq!(schema_id, 3);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_ipc_bytes_accessor() {
        let cmd = ConsensusCommand::RegisterMasterBatch {
            batch_id: 1,
            component: "x".to_string(),
            schema_id: 1,
            ipc_bytes: vec![0xFF],
            total_rows: 1,
            now_at_propose: 0,
        };
        assert_eq!(cmd.ipc_bytes(), Some(vec![0xFF].as_slice()));

        let cmd2 = ConsensusCommand::AckClaim {
            claim_id: Uuid::now_v7(),
            instance_id: Uuid::now_v7(),
        };
        assert_eq!(cmd2.ipc_bytes(), None);
    }

    #[test]
    fn test_consensus_response_serde_round_trip() {
        let claim_id = Uuid::now_v7();
        let instance_id = Uuid::now_v7();
        let resp = ConsensusResponse::BatchClaimed {
            batch_id: 5,
            component: "items".to_string(),
            row_range_start: 10,
            row_range_end: 20,
            schema_id: 2,
            claim_id,
            instance_id,
            lease_expires_at: 99999,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: ConsensusResponse = serde_json::from_str(&json).unwrap();
        assert!(matches!(
            decoded,
            ConsensusResponse::BatchClaimed { batch_id: 5, .. }
        ));
    }
}
