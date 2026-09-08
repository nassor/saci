//! TCP transport for Arrow-IPC Raft consensus messages.
//!
//! Length-prefixed framing with a typed envelope that distinguishes message kinds on
//! the wire:
//!
//! - Control messages (`AppendEntries`, `Vote`) are serialised as `serde_json`.
//! - Snapshot transfer uses a multi-frame chunked protocol (4 MiB per chunk).
//!
//! ```text
//! ┌────────────────┬──────────────────────────┐
//! │  length: u32   │  payload: [u8; length]   │
//! │  (big-endian)  │                          │
//! └────────────────┴──────────────────────────┘
//! ```
//!
//! Each payload is a `serde_json`-encoded `RpcEnvelope`. The wire format is
//! **append-only**: existing variant positions must never change, so rolling upgrades
//! stay compatible.
//!
//! ## Layout
//!
//! This file owns the shared frame and snapshot constants plus the [`TransportError`]
//! classification. `wire` holds the on-wire message types and the frame codec,
//! `client` the outbound half ([`TcpNetwork`], the per-peer pool, proposal
//! forwarding), and `server` the inbound half ([`RaftTcpServer`]).
//!
//! [`RaftTcpServer`] binds a listen address and dispatches incoming envelopes to the
//! local [`Raft`](openraft::Raft) node. Start it once during node initialisation,
//! before any remote peer can contact the node.
//!
//! [`TcpNetwork`] keeps a per-peer pool of idle
//! [`TcpStream`](tokio::net::TcpStream)s bounded by [`POOL_CAPACITY`]. Streams are
//! acquired from the pool or freshly connected, returned on success, and dropped on
//! error so a broken stream never re-enters the pool.
//!
//! Read-frame calls on the RPC-response path use [`tokio::time::timeout`] with a
//! deadline of [`RPC_READ_TIMEOUT`], connect attempts use [`CONNECT_TIMEOUT`], and
//! write calls use [`RPC_WRITE_TIMEOUT`].

mod client;
mod server;
mod wire;

#[cfg(feature = "distributed-raft")]
use std::io;
#[cfg(feature = "distributed-raft")]
use std::time::Duration;

#[cfg(feature = "distributed-raft")]
use openraft::error::{NetworkError, RPCError, StreamingError, Unreachable};

#[cfg(feature = "distributed-raft")]
use crate::distributed::consensus::types::SaciTypeConfig;

#[cfg(feature = "distributed-raft")]
pub use client::TcpNetwork;
pub use client::TcpNetworkFactory;
#[cfg(feature = "distributed-raft")]
pub(crate) use client::forward_proposal;
#[cfg(feature = "distributed-raft")]
pub use server::RaftTcpServer;
#[cfg(feature = "distributed-raft")]
pub(crate) use wire::{RpcEnvelope, RpcResponse, SnapshotChunkMsg, SnapshotFinalMsg};

/// Hard cap on a single TCP frame. Snapshot *chunks* are bounded by
/// [`SNAPSHOT_CHUNK_BYTES`], which is well within this limit.
#[cfg_attr(not(feature = "distributed-raft"), allow(dead_code))]
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024; // 16 MiB

/// Maximum snapshot payload per chunk frame.
///
/// Chosen to keep each frame well below `MAX_FRAME_BYTES` while limiting the
/// number of round-trips for typical state-machine snapshots (< 64 MiB).
#[cfg(feature = "distributed-raft")]
pub const SNAPSHOT_CHUNK_BYTES: usize = 4 * 1024 * 1024; // 4 MiB

/// Per-RPC read-response timeout. A peer that accepted the TCP connect but never
/// replies is declared unreachable.
#[cfg(feature = "distributed-raft")]
pub const RPC_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Per-RPC write timeout. A blocked-but-alive peer, with a full TCP send buffer, is
/// declared unreachable.
#[cfg(feature = "distributed-raft")]
pub const RPC_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Read-response timeout used exclusively for [`forward_proposal`].
///
/// Proposal forwarding waits for the remote leader to run `client_write`, which can
/// block for a full Raft commit round-trip. Must be strictly greater than
/// `CLUSTER_PROPOSE_TIMEOUT` (30 s) so the store layer's outer timeout fires first.
#[cfg(feature = "distributed-raft")]
const PROPOSAL_FORWARD_READ_TIMEOUT: Duration = Duration::from_secs(35);

/// Timeout for establishing a new TCP connection to a peer.
#[cfg(feature = "distributed-raft")]
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// Maximum number of idle connections kept per peer.
#[cfg(feature = "distributed-raft")]
pub const POOL_CAPACITY: usize = 4;

/// Maximum idle time for a pooled connection before it is dropped on next acquire.
#[cfg(feature = "distributed-raft")]
const POOL_MAX_IDLE: Duration = Duration::from_secs(10);

/// Maximum number of concurrent accepted connections on the server.
#[cfg(feature = "distributed-raft")]
const MAX_ACCEPTED_CONNECTIONS: usize = 1024;

/// Maximum total bytes buffered per in-flight snapshot transfer (256 MiB).
#[cfg(feature = "distributed-raft")]
const SNAPSHOT_MAX_TRANSFER_BYTES: usize = 256 * 1024 * 1024;

/// Maximum number of concurrent in-flight snapshot transfers per connection.
#[cfg(feature = "distributed-raft")]
const SNAPSHOT_MAX_CONCURRENT_TRANSFERS: usize = 4;

/// Idle timeout for a snapshot transfer (no chunk received for this duration).
#[cfg(feature = "distributed-raft")]
const SNAPSHOT_TRANSFER_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Idle-read timeout on the server connection task.
///
/// A peer that keeps TCP alive but stops sending is evicted. Well above Raft
/// heartbeat intervals, typically 150-500 ms.
#[cfg(feature = "distributed-raft")]
const IDLE_READ_TIMEOUT: Duration = Duration::from_secs(60);

/// Maximum per-chunk byte size enforced on the client before framing.
///
/// Matches the server-side cap so oversized chunks are caught client-side with
/// a clear error rather than being rejected by the server after framing.
#[cfg(feature = "distributed-raft")]
const MAX_SNAPSHOT_CHUNK_BYTES: usize = SNAPSHOT_CHUNK_BYTES; // 4 MiB

/// Number of consecutive failed RPCs on an established stream before a
/// per-peer circuit opens.
///
/// A refused or timed-out connect never counts: it leaves
/// `PeerPool::acquire` before a stream exists, surfaces as
/// [`RPCError::Unreachable`](openraft::error::RPCError), and is paced by
/// openraft's backoff instead.
#[cfg(feature = "distributed-raft")]
const CIRCUIT_OPEN_THRESHOLD: u32 = 5;

/// Duration a circuit stays open before the next attempt is let through.
///
/// openraft drives replication one heartbeat apart, so a peer that accepts
/// connections but fails every RPC on them reaches
/// [`CIRCUIT_OPEN_THRESHOLD`] within a few hundred milliseconds. The window
/// is not the whole recovery bound. `PeerPool::acquire` reports its fast-fail
/// to openraft as a `Network` error, and openraft throttles replication once
/// the accumulated rank of consecutive errors passes its threshold, so a
/// short burst of fast-fails engages that target's
/// [`Backoff`](openraft::network::Backoff) ramp (`TcpNetwork::backoff`:
/// 100 ms doubling to a 10 s cap, plus up to 20% jitter) and every fast-fail
/// after it costs a step. A peer whose network has already healed therefore
/// waits for the window to elapse and then for the next backoff step to come
/// due, up to 12 s beyond it.
///
/// 2 s keeps that total inside the same order as Raft's own timescale, which
/// runs a 50 ms heartbeat and a 150 to 300 ms election timeout
/// ([`ArrowRaftDriverConfig`](crate::distributed::consensus::ArrowRaftDriverConfig)).
/// A window of tens of seconds instead strands an already-recovered follower
/// for tens of seconds, long enough to outlast a whole election and every
/// commit in it. The flat shape is deliberate: openraft owns the exponential
/// ramp per target, and a growing window here would compound with it and
/// stretch recovery for exactly the peer that just came back.
///
/// The price of a short window is a flapping peer, which draws a fresh burst
/// of [`CIRCUIT_OPEN_THRESHOLD`] failed RPCs every 2 s.
#[cfg(feature = "distributed-raft")]
const CIRCUIT_OPEN_DURATION: Duration = Duration::from_secs(2);

/// Fine-grained transport error, mapped to the appropriate openraft error type.
///
/// Mapping table:
///
/// | `TransportError` variant    | openraft mapping                          | Semantic                            |
/// |-----------------------------|-------------------------------------------|-------------------------------------|
/// | `ConnectFailed`             | `RPCError::Unreachable`                   | Peer is down / unreachable          |
/// | `WriteFailed`               | `RPCError::Network` (transient)           | Lost connection mid-send            |
/// | `WriteTimeout`              | `RPCError::Network` (transient)           | Peer alive but write buffer full    |
/// | `ReadTimeout`               | `RPCError::Network` (transient)           | Peer alive but not responding       |
/// | `FramingError`              | `RPCError::Network` (transient)           | Corrupt/truncated frame             |
/// | `PeerReset`                 | `RPCError::Network` (transient)           | Peer closed connection cleanly      |
/// | `EncodeError`               | `RPCError::Unreachable` (fatal-ish)       | Serialization bug, not transient    |
/// | `Other`                     | `RPCError::Network` (transient)           | Miscellaneous I/O error             |
#[cfg(feature = "distributed-raft")]
#[derive(Debug)]
pub enum TransportError {
    /// TCP connect failed (peer unreachable or connection refused).
    ConnectFailed(io::Error),
    /// Write to stream failed.
    WriteFailed(io::Error),
    /// Write timed out; the peer send buffer is full.
    WriteTimeout,
    /// Read timed out: no response within [`RPC_READ_TIMEOUT`].
    ReadTimeout,
    /// Frame protocol error (oversized frame, premature EOF).
    FramingError(String),
    /// Peer reset the connection cleanly (EOF on read).
    PeerReset,
    /// Serialization error: a bug, not a transient network issue.
    EncodeError(String),
    /// Other I/O error.
    Other(io::Error),
}

#[cfg(feature = "distributed-raft")]
impl TransportError {
    /// Map to openraft `RPCError`. Connect failures and encode errors surface
    /// as `Unreachable`, everything else as `Network`, a fault on a live
    /// stream. openraft weights the two: replication throttles once the
    /// accumulated rank of consecutive errors passes 20, and `Unreachable`
    /// counts 100 against `Network`'s 2, so an unreachable peer is paced from
    /// its first error while a live-stream fault takes eleven in a row. Any
    /// success clears the rank.
    pub fn into_rpc_error(self) -> RPCError<SaciTypeConfig> {
        match self {
            TransportError::ConnectFailed(e) => {
                RPCError::Unreachable(Unreachable::from_string(format!("connect failed: {e}")))
            }
            TransportError::EncodeError(msg) => RPCError::Unreachable(Unreachable::from_string(
                format!("encode error (bug): {msg}"),
            )),
            TransportError::ReadTimeout => {
                RPCError::Network(NetworkError::new(&io::Error::other("RPC read timeout")))
            }
            TransportError::WriteTimeout => {
                RPCError::Network(NetworkError::new(&io::Error::other("RPC write timeout")))
            }
            TransportError::WriteFailed(e) => RPCError::Network(NetworkError::new(
                &io::Error::other(format!("write failed: {e}")),
            )),
            TransportError::FramingError(msg) => {
                RPCError::Network(NetworkError::new(&io::Error::other(msg)))
            }
            TransportError::PeerReset => RPCError::Network(NetworkError::new(&io::Error::other(
                "peer reset connection",
            ))),
            TransportError::Other(e) => RPCError::Network(NetworkError::new(&e)),
        }
    }

    pub fn into_streaming_error(self) -> StreamingError<SaciTypeConfig> {
        StreamingError::from(self.into_rpc_error())
    }
}

#[cfg(test)]
mod tests {
    // Only the Raft-gated tests below name items from this module; the helpers
    // exist for the sibling test modules.
    #[cfg(feature = "distributed-raft")]
    use super::*;
    use std::net::SocketAddr;
    use tokio::net::TcpListener;

    use super::wire::{read_frame, write_frame};

    pub(super) fn free_addr() -> SocketAddr {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    }

    pub(super) fn spawn_echo_server(addr: SocketAddr) {
        tokio::spawn(async move {
            let listener = TcpListener::bind(addr).await.unwrap();
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    while let Ok(Some(frame)) = read_frame(&mut stream).await {
                        let _ = write_frame(&mut stream, &frame).await;
                    }
                });
            }
        });
    }

    /// `PeerReset`, `WriteFailed`, `FramingError`, and `Other` must all map to
    /// the transient `RPCError::Network` variant.
    #[cfg(feature = "distributed-raft")]
    #[tokio::test]
    async fn test_transient_errors_map_to_network() {
        let cases: Vec<TransportError> = vec![
            TransportError::PeerReset,
            TransportError::WriteFailed(io::Error::other("x")),
            TransportError::WriteTimeout,
            TransportError::FramingError("bad frame".to_string()),
            TransportError::Other(io::Error::other("y")),
        ];
        for err in cases {
            let rpc_err = err.into_rpc_error();
            assert!(
                matches!(rpc_err, RPCError::Network(_)),
                "expected Network for transient error, got: {rpc_err:?}"
            );
        }
    }

    /// `EncodeError` must map to `RPCError::Unreachable`, since serialization failures
    /// are bugs rather than network hiccups.
    #[cfg(feature = "distributed-raft")]
    #[test]
    fn test_encode_error_maps_to_unreachable() {
        let err = TransportError::EncodeError("bad type".to_string());
        let rpc_err = err.into_rpc_error();
        assert!(
            matches!(rpc_err, RPCError::Unreachable(_)),
            "encode error must map to Unreachable, got: {rpc_err:?}"
        );
    }
}
