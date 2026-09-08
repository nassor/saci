//! Arrow-IPC distributed execution layer for SACI.
//!
//! Distributed batch processing runs natively on Apache Arrow IPC. Partitions are
//! claimed as row ranges of a replicated master `RecordBatch`, and every persisted
//! artefact (checkpoints, window accumulators, runtime-state blobs) crosses the wire
//! as IPC bytes.
//!
//! # Feature gates
//!
//! - `distributed`: this module and the core traits.
//! - `distributed-raft`: adds openraft consensus.
//!
//! # Architecture
//!
//! ```text
//! PartitionSource  ─────────────────────────────►  claim row-ranges
//! CheckpointStore  ─────────────────────────────►  persist IPC snapshots
//! DistributedRunner ─ claim → run_on_with_state → checkpoint → ack
//! RedbSharedStore  ─ single-node or multi-node (via Raft channel)
//! consensus/            ─ state machine, log store, snapshot, driver, transport
//! ```
//!
//! # Log entry size constraint
//!
//! Arrow IPC payloads embedded in Raft log entries are bounded at 1 MiB
//! ([`MAX_LOG_ENTRY_BYTES`]); larger payloads are rejected at the propose boundary.
//! The snapshot path (openraft `build_snapshot` / `install_snapshot`) handles
//! arbitrarily large state.

pub mod accumulator_store;
pub mod checkpoint;
pub mod consensus;
pub mod partition;
pub mod processor_state_store;
pub mod runner;
pub mod strategy;

pub use checkpoint::{
    ACCUMULATOR_STAGE_SENTINEL, Checkpoint, CheckpointStore, PROCESSOR_STATE_STAGE_SENTINEL,
};
pub use consensus::RedbSharedStore;
pub use partition::{BatchClaim, MAX_LOG_ENTRY_BYTES, PartitionSource};
pub use runner::{DistributedRunner, KeyPartition, RunnerConfig};
pub use strategy::CheckpointStrategy;

// Parquet archival checkpoint store; requires both `distributed` and
// `parquet-checkpoint`.
#[cfg(feature = "parquet-checkpoint")]
pub mod parquet_checkpoint;
#[cfg(feature = "parquet-checkpoint")]
pub use parquet_checkpoint::ParquetCheckpointStore;
