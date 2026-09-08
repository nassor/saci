//! Command handlers for the consensus state machine.
//!
//! One module per command family: `batch` for master-batch registration,
//! poisoning, and the release-attempt counter; `claim` for the claim lifecycle
//! and the instance heartbeat; `checkpoint` for stage checkpoint writes. Every
//! handler is invoked from the [`apply`](super::apply) dispatcher, opens its own
//! redb transaction, and reports expected application-level conditions as
//! `ConsensusResponse::Error` rather than `Err`.

pub(super) mod batch;
pub(super) mod checkpoint;
pub(super) mod claim;

pub(super) use batch::{apply_poison_batch, apply_register_master_batch};
pub(super) use checkpoint::apply_checkpoint;
pub(super) use claim::{
    apply_ack_claim, apply_claim_row_range, apply_heartbeat, apply_reclaim_expired,
    apply_release_claim, apply_renew_claim,
};
