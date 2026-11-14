//! On-wire message types and the length-prefixed frame codec.
//!
//! Holds the append-only [`RpcEnvelope`] / [`RpcResponse`] pair exchanged by
//! every RPC, the snapshot chunk messages, and the [`read_frame`] /
//! [`write_frame`] helpers both directions share.

use std::io;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[cfg(feature = "distributed-raft")]
use openraft::{
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
    },
    type_config::alias::{SnapshotMetaOf, VoteOf},
};
#[cfg(feature = "distributed-raft")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "distributed-raft")]
use crate::distributed::consensus::types::{ConsensusCommand, ConsensusResponse, SaciTypeConfig};

use super::MAX_FRAME_BYTES;

/// Typed envelope for all RPCs sent over the TCP transport.
///
/// **Append-only**: do not reorder or remove variants. The `serde_json` discriminant
/// is the variant name string, so adding new variants at the end is always safe.
#[cfg(feature = "distributed-raft")]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum RpcEnvelope {
    /// `AppendEntries` RPC.
    AppendEntries(AppendEntriesRequest<SaciTypeConfig>),
    /// `Vote` / `RequestVote` RPC.
    Vote(VoteRequest<SaciTypeConfig>),
    /// One chunk of a snapshot transfer.
    SnapshotChunk(SnapshotChunkMsg),
    /// Signals the last chunk and carries the snapshot metadata.
    SnapshotFinal(SnapshotFinalMsg),
    /// A follower forwards a proposal to the leader.
    ///
    /// Sits at the end of the enum to keep wire-format compatibility with older nodes.
    ProposalForward { command: ConsensusCommand },
}

/// A single data chunk within a snapshot transfer.
#[cfg(feature = "distributed-raft")]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SnapshotChunkMsg {
    /// Unique transfer ID shared across all chunks of one snapshot send.
    pub transfer_id: u64,
    /// Byte offset within the full snapshot payload.
    pub offset: u64,
    /// Raw bytes of this chunk.
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

/// Final (or only) chunk of a snapshot transfer; includes metadata.
#[cfg(feature = "distributed-raft")]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SnapshotFinalMsg {
    /// Unique transfer ID shared across all chunks of one snapshot send.
    pub transfer_id: u64,
    /// Byte offset of the last chunk's start.
    pub offset: u64,
    /// Raw bytes of the last chunk (may be empty).
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
    /// Leader vote, forwarded to [`Raft::install_full_snapshot`].
    pub vote: VoteOf<SaciTypeConfig>,
    /// Snapshot metadata.
    pub meta: SnapshotMetaOf<SaciTypeConfig>,
}

/// Response envelope returned from the server for each incoming RPC.
#[cfg(feature = "distributed-raft")]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum RpcResponse {
    /// Response to an `AppendEntries` RPC.
    AppendEntries(AppendEntriesResponse<SaciTypeConfig>),
    /// Response to a `Vote` RPC.
    Vote(VoteResponse<SaciTypeConfig>),
    /// Acknowledgement for an intermediate snapshot chunk.
    SnapshotChunkAck { transfer_id: u64 },
    /// Final response after the snapshot was installed.
    SnapshotDone(SnapshotResponse<SaciTypeConfig>),
    /// Error string returned by the server.
    Error(String),
    /// Result of a forwarded proposal. Uses `Option` fields instead of
    /// `Result` to keep serde_json serialization clean.
    ///
    /// Exactly one of `ok` and `err` is `Some`.
    ProposalResult {
        ok: Option<ConsensusResponse>,
        err: Option<String>,
    },
}

/// Read one length-prefixed frame from `stream`.
///
/// Returns:
/// - `Ok(Some(bytes))` when a complete frame was received.
/// - `Ok(None)` when the peer closed the connection cleanly (EOF on length header).
/// - `Err(e)` on I/O error:
///   - `ErrorKind::InvalidData` when the frame length exceeds [`MAX_FRAME_BYTES`].
///   - `ErrorKind::UnexpectedEof` on a truncated frame (EOF inside payload).
///   - Other kinds forwarded from the underlying stream.
#[cfg_attr(not(feature = "distributed-raft"), allow(dead_code))]
pub(super) async fn read_frame(stream: &mut TcpStream) -> io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match stream.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame too large: {len} > {MAX_FRAME_BYTES}"),
        ));
    }
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await.map_err(|e| {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            io::Error::new(io::ErrorKind::UnexpectedEof, "truncated frame payload")
        } else {
            e
        }
    })?;
    Ok(Some(payload))
}

#[cfg_attr(not(feature = "distributed-raft"), allow(dead_code))]
pub(super) async fn write_frame(stream: &mut TcpStream, data: &[u8]) -> io::Result<()> {
    if data.len() > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame too large: {} > {MAX_FRAME_BYTES}", data.len()),
        ));
    }
    let len = u32::try_from(data.len()).map_err(|_| io::Error::other("frame too large"))?;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(data).await?;
    stream.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::net::TcpListener;

    use super::super::tests::{free_addr, spawn_echo_server};

    #[test]
    fn test_serde_command_round_trip_via_json() {
        use crate::distributed::consensus::types::ConsensusCommand;
        let cmd = ConsensusCommand::AckClaim {
            claim_id: uuid::Uuid::now_v7(),
            instance_id: uuid::Uuid::now_v7(),
        };
        let json = serde_json::to_vec(&cmd).unwrap();
        let decoded: ConsensusCommand = serde_json::from_slice(&json).unwrap();
        assert!(matches!(decoded, ConsensusCommand::AckClaim { .. }));
    }

    /// Oversized frame returns InvalidData, not silently truncates.
    #[tokio::test]
    async fn test_read_frame_oversized_returns_error() {
        use tokio::io::AsyncWriteExt;
        let addr = free_addr();
        tokio::spawn(async move {
            let listener = TcpListener::bind(addr).await.unwrap();
            if let Ok((mut stream, _)) = listener.accept().await {
                // Send a length larger than MAX_FRAME_BYTES.
                let oversized_len = (MAX_FRAME_BYTES + 1) as u32;
                let _ = stream.write_all(&oversized_len.to_be_bytes()).await;
            }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut stream = TcpStream::connect(addr).await.unwrap();
        let result = read_frame(&mut stream).await;
        assert!(
            matches!(&result, Err(e) if e.kind() == io::ErrorKind::InvalidData),
            "oversized frame must return InvalidData, got: {result:?}"
        );
    }

    /// Truncated frame payload returns UnexpectedEof.
    #[tokio::test]
    async fn test_read_frame_truncated_returns_unexpected_eof() {
        use tokio::io::AsyncWriteExt;
        let addr = free_addr();
        tokio::spawn(async move {
            let listener = TcpListener::bind(addr).await.unwrap();
            if let Ok((mut stream, _)) = listener.accept().await {
                // Claim 10 bytes but only send 5.
                let len: u32 = 10;
                let _ = stream.write_all(&len.to_be_bytes()).await;
                let _ = stream.write_all(b"hello").await;
                // Dropping the stream causes EOF mid-payload.
            }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut stream = TcpStream::connect(addr).await.unwrap();
        let result = read_frame(&mut stream).await;
        assert!(
            matches!(&result, Err(e) if e.kind() == io::ErrorKind::UnexpectedEof),
            "truncated frame must return UnexpectedEof, got: {result:?}"
        );
    }

    /// write_frame rejects frames larger than MAX_FRAME_BYTES before writing.
    #[tokio::test]
    async fn test_write_frame_oversized_returns_error() {
        let addr = free_addr();
        spawn_echo_server(addr);
        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut stream = TcpStream::connect(addr).await.unwrap();
        // MAX_FRAME_BYTES + 1 bytes.
        let big = vec![0u8; MAX_FRAME_BYTES + 1];
        let result = write_frame(&mut stream, &big).await;
        assert!(
            matches!(&result, Err(e) if e.kind() == io::ErrorKind::InvalidData),
            "oversized write must return InvalidData, got: {result:?}"
        );
    }

    /// clean EOF (peer closes without sending anything) returns Ok(None).
    #[tokio::test]
    async fn test_read_frame_clean_eof_returns_none() {
        let addr = free_addr();
        tokio::spawn(async move {
            let listener = TcpListener::bind(addr).await.unwrap();
            if let Ok((_stream, _)) = listener.accept().await {
                // Dropping the stream immediately sends a clean FIN.
            }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut stream = TcpStream::connect(addr).await.unwrap();
        let result = read_frame(&mut stream).await;
        assert!(
            matches!(result, Ok(None)),
            "clean EOF must return Ok(None), got: {result:?}"
        );
    }

    /// `handle_envelope` with `ProposalForward` returns `ProposalResult` through the
    /// wire framing end to end, checking the serde round-trip.
    #[cfg(feature = "distributed-raft")]
    #[test]
    fn test_proposal_forward_envelope_serde_round_trip() {
        use uuid::Uuid;

        let cmd = ConsensusCommand::AckClaim {
            claim_id: Uuid::now_v7(),
            instance_id: Uuid::now_v7(),
        };
        let envelope = RpcEnvelope::ProposalForward {
            command: cmd.clone(),
        };
        let json = serde_json::to_vec(&envelope).unwrap();
        let decoded: RpcEnvelope = serde_json::from_slice(&json).unwrap();
        assert!(
            matches!(decoded, RpcEnvelope::ProposalForward { .. }),
            "should decode back to ProposalForward"
        );

        let resp = RpcResponse::ProposalResult {
            ok: Some(ConsensusResponse::ClaimAcked),
            err: None,
        };
        let json = serde_json::to_vec(&resp).unwrap();
        let decoded: RpcResponse = serde_json::from_slice(&json).unwrap();
        assert!(
            matches!(
                decoded,
                RpcResponse::ProposalResult {
                    ok: Some(ConsensusResponse::ClaimAcked),
                    ..
                }
            ),
            "should decode back to ProposalResult with ok"
        );
    }
}
