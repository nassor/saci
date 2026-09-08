//! Arrow-IPC consensus layer for SACI's distributed execution.
//!
//! Holds the redb-backed state machine, optional openraft integration, and the TCP
//! transport for the Arrow distributed layer.
//!
//! # Feature gates
//!
//! - `distributed`: enables core types, state machine, and store.
//! - `distributed-raft`: additionally enables openraft log storage,
//!   state machine, snapshot builder, driver, and transport.

pub mod state_machine;
pub mod store;
pub mod transport;
pub mod types;

#[cfg(feature = "distributed-raft")]
pub mod driver;
#[cfg(feature = "distributed-raft")]
pub mod snapshot;
#[cfg(feature = "distributed-raft")]
pub mod storage;

pub use state_machine::apply;
pub use store::RedbSharedStore;
pub use types::{ConsensusCommand, ConsensusResponse};

#[cfg(feature = "distributed-raft")]
pub use driver::{ArrowRaftDriver, ArrowRaftDriverConfig, ArrowRaftDriverHandle};
#[cfg(feature = "distributed-raft")]
pub use snapshot::{build_snapshot_bytes, install_snapshot_bytes};
#[cfg(feature = "distributed-raft")]
pub use storage::{ArrowRedbLogStore, ArrowRedbStateMachine};
