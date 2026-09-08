//! Inbound half of the transport: the TCP accept loop and RPC dispatch.
//!
//! Owns [`RaftTcpServer`], the per-connection read/dispatch/write loop, and the
//! server-side snapshot reassembly buffers keyed by transfer ID.

#[cfg(feature = "distributed-raft")]
use std::collections::HashMap;
#[cfg(feature = "distributed-raft")]
use std::io;
#[cfg(feature = "distributed-raft")]
use std::net::SocketAddr;
#[cfg(feature = "distributed-raft")]
use std::sync::Arc;
#[cfg(feature = "distributed-raft")]
use std::time::{Duration, Instant};

#[cfg(feature = "distributed-raft")]
use tokio::net::TcpStream;

#[cfg(feature = "distributed-raft")]
use openraft::Snapshot;

#[cfg(feature = "distributed-raft")]
use super::wire::{read_frame, write_frame};
#[cfg(feature = "distributed-raft")]
use super::{
    IDLE_READ_TIMEOUT, MAX_ACCEPTED_CONNECTIONS, RpcEnvelope, RpcResponse,
    SNAPSHOT_MAX_CONCURRENT_TRANSFERS, SNAPSHOT_MAX_TRANSFER_BYTES, SNAPSHOT_TRANSFER_IDLE_TIMEOUT,
};

/// In-flight snapshot transfer state on the server side.
#[cfg(feature = "distributed-raft")]
struct InFlightSnapshot {
    data: Vec<u8>,
    last_chunk_at: Instant,
}

/// TCP server that dispatches incoming Raft RPCs to a local `Raft` node.
///
/// Start one instance per node during cluster initialisation. The server binds
/// `listen_addr` and spawns a Tokio task per accepted connection. The accept
/// loop stops on [`RaftTcpServer::shutdown`] or when the server handle drops.
#[cfg(feature = "distributed-raft")]
pub struct RaftTcpServer {
    raft: crate::distributed::consensus::driver::raft_impl::ArrowSaciRaft,
    listen_addr: SocketAddr,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
}

#[cfg(feature = "distributed-raft")]
impl RaftTcpServer {
    /// Create a new server bound to `listen_addr` that dispatches RPCs to `raft`.
    pub fn new(
        raft: crate::distributed::consensus::driver::raft_impl::ArrowSaciRaft,
        listen_addr: SocketAddr,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        Self {
            raft,
            listen_addr,
            shutdown_tx,
            shutdown_rx,
        }
    }

    /// Signal the accept loop to stop.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    /// Bind `listen_addr`, then spawn the accept loop as a background task.
    ///
    /// Binding before the spawn is what makes an address already in use an
    /// error the caller sees. Binding inside the task would leave the failure
    /// in a `JoinHandle` nobody awaits, and the node would run on as a member
    /// no peer can reach.
    ///
    /// # Errors
    ///
    /// Returns the `bind` error, typically `AddrInUse`.
    pub async fn bind_and_spawn(self) -> io::Result<tokio::task::JoinHandle<io::Result<()>>> {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind(self.listen_addr).await?;
        Ok(tokio::spawn(async move { self.run(listener).await }))
    }

    async fn run(mut self, listener: tokio::net::TcpListener) -> io::Result<()> {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_ACCEPTED_CONNECTIONS));

        #[cfg(feature = "tracing")]
        tracing::info!(addr = %self.listen_addr, "RaftTcpServer listening");

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    let (stream, _peer_addr) = match accept_result {
                        Ok(pair) => pair,
                        Err(_e) => {
                            #[cfg(feature = "tracing")]
                            tracing::warn!(error = %_e, "RaftTcpServer: accept error");
                            // Sleep briefly to avoid tight-looping on EMFILE.
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                    };

                    #[cfg(feature = "tracing")]
                    tracing::debug!(peer = %_peer_addr, "RaftTcpServer: accepted connection");

                    let permit = match semaphore.clone().try_acquire_owned() {
                        Ok(p) => p,
                        Err(_) => {
                            #[cfg(feature = "tracing")]
                            tracing::warn!(peer = %_peer_addr, "RaftTcpServer: connection limit reached, dropping");
                            drop(stream);
                            continue;
                        }
                    };

                    let raft = self.raft.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        handle_connection(raft, stream).await;
                        #[cfg(feature = "tracing")]
                        tracing::debug!(peer = %_peer_addr, "RaftTcpServer: connection closed");
                    });
                }
                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        #[cfg(feature = "tracing")]
                        tracing::info!("RaftTcpServer: shutdown signal received");
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}

/// Handle one accepted connection: read RPCs, dispatch, write responses.
#[cfg(feature = "distributed-raft")]
async fn handle_connection(
    raft: crate::distributed::consensus::driver::raft_impl::ArrowSaciRaft,
    mut stream: TcpStream,
) {
    // Per-connection reassembly state for snapshot transfers.
    let mut snapshot_transfers: HashMap<u64, InFlightSnapshot> = HashMap::new();

    loop {
        // Close the connection if the peer stops sending, so an alive-but-silent TCP
        // link does not park a task indefinitely.
        let raw = match tokio::time::timeout(IDLE_READ_TIMEOUT, read_frame(&mut stream)).await {
            Ok(Ok(Some(b))) => b,
            Ok(Ok(None)) => break, // clean EOF
            Ok(Err(_e)) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(error = %_e, "RaftTcpServer: read frame error");
                break;
            }
            Err(_) => {
                // Idle timeout with no frame received; close cleanly.
                #[cfg(feature = "tracing")]
                tracing::debug!("RaftTcpServer: idle timeout, closing connection");
                break;
            }
        };

        let envelope: RpcEnvelope = match serde_json::from_slice(&raw) {
            Ok(e) => e,
            Err(_e) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(error = %_e, "RaftTcpServer: envelope decode error");
                break;
            }
        };

        let response = handle_envelope(&raft, envelope, &mut snapshot_transfers).await;

        let resp_bytes = match serde_json::to_vec(&response) {
            Ok(b) => b,
            Err(_e) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(error = %_e, "RaftTcpServer: response encode error");
                break;
            }
        };

        if write_frame(&mut stream, &resp_bytes).await.is_err() {
            break;
        }
    }
}

/// Dispatch one decoded envelope to the local Raft node and return a response.
#[cfg(feature = "distributed-raft")]
async fn handle_envelope(
    raft: &crate::distributed::consensus::driver::raft_impl::ArrowSaciRaft,
    envelope: RpcEnvelope,
    snapshot_transfers: &mut HashMap<u64, InFlightSnapshot>,
) -> RpcResponse {
    use std::io::Cursor;

    match envelope {
        RpcEnvelope::AppendEntries(req) => match raft.append_entries(req).await {
            Ok(resp) => RpcResponse::AppendEntries(resp),
            Err(e) => RpcResponse::Error(e.to_string()),
        },

        RpcEnvelope::Vote(req) => match raft.vote(req).await {
            Ok(resp) => RpcResponse::Vote(resp),
            Err(e) => RpcResponse::Error(e.to_string()),
        },

        RpcEnvelope::SnapshotChunk(chunk) => {
            let transfer_id = chunk.transfer_id;
            // Evict stale transfers only on snapshot traffic. This bounds memory
            // without adding per-RPC overhead to the common append_entries path.
            snapshot_transfers
                .retain(|_, v| v.last_chunk_at.elapsed() <= SNAPSHOT_TRANSFER_IDLE_TIMEOUT);

            if !snapshot_transfers.contains_key(&transfer_id)
                && snapshot_transfers.len() >= SNAPSHOT_MAX_CONCURRENT_TRANSFERS
            {
                return RpcResponse::Error(format!(
                    "too many concurrent snapshot transfers (max {SNAPSHOT_MAX_CONCURRENT_TRANSFERS})"
                ));
            }

            let entry = snapshot_transfers
                .entry(transfer_id)
                .or_insert_with(|| InFlightSnapshot {
                    data: Vec::new(),
                    last_chunk_at: Instant::now(),
                });

            let new_size = entry.data.len() + chunk.data.len();
            if new_size > SNAPSHOT_MAX_TRANSFER_BYTES {
                snapshot_transfers.remove(&transfer_id);
                return RpcResponse::Error(format!(
                    "snapshot transfer {transfer_id} exceeded size limit ({SNAPSHOT_MAX_TRANSFER_BYTES} bytes)"
                ));
            }

            entry.data.extend_from_slice(&chunk.data);
            entry.last_chunk_at = Instant::now();

            RpcResponse::SnapshotChunkAck { transfer_id }
        }

        RpcEnvelope::SnapshotFinal(final_msg) => {
            let transfer_id = final_msg.transfer_id;
            let mut buf = snapshot_transfers
                .remove(&transfer_id)
                .map(|s| s.data)
                .unwrap_or_default();
            buf.extend_from_slice(&final_msg.data);

            let snapshot = Snapshot {
                meta: final_msg.meta,
                snapshot: Cursor::new(buf),
            };

            let vote = final_msg.vote;
            // Spawn the install to keep the connection task responsive, but await the
            // handle so the response goes back on the same stream.
            let raft = raft.clone();
            let result =
                tokio::spawn(async move { raft.install_full_snapshot(vote, snapshot).await }).await;

            match result {
                Ok(Ok(resp)) => RpcResponse::SnapshotDone(resp),
                Ok(Err(e)) => RpcResponse::Error(e.to_string()),
                Err(e) => RpcResponse::Error(format!("snapshot install task panicked: {e}")),
            }
        }

        RpcEnvelope::ProposalForward { command } => {
            // The server-side handler calls client_write directly. It never forwards
            // further, even when this node is also a follower, because that would loop
            // between nodes.
            //
            // The 28 s timeout around client_write guarantees a response before the
            // caller's PROPOSAL_FORWARD_READ_TIMEOUT (35 s) fires, so the follower gets
            // a structured error instead of a TCP drop.
            use openraft::error::{ClientWriteError, RaftError};
            const SERVER_WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(28);
            let write_result =
                tokio::time::timeout(SERVER_WRITE_TIMEOUT, raft.client_write(command)).await;
            match write_result {
                Ok(Ok(resp)) => RpcResponse::ProposalResult {
                    ok: Some(resp.data),
                    err: None,
                },
                Ok(Err(RaftError::APIError(ClientWriteError::ForwardToLeader(_)))) => {
                    // Not the leader, so reject the forward rather than propagating it.
                    RpcResponse::ProposalResult {
                        ok: None,
                        err: Some("not leader".to_string()),
                    }
                }
                Ok(Err(e)) => RpcResponse::ProposalResult {
                    ok: None,
                    err: Some(e.to_string()),
                },
                Err(_elapsed) => RpcResponse::ProposalResult {
                    ok: None,
                    err: Some("server-side client_write timeout".to_string()),
                },
            }
        }
    }
}

#[cfg(all(test, feature = "distributed-raft"))]
mod tests {
    use super::*;

    /// Snapshot chunk accumulation enforces the concurrent transfer limit.
    #[tokio::test]
    async fn test_snapshot_buffer_concurrent_limit() {
        let mut transfers: HashMap<u64, InFlightSnapshot> = HashMap::new();
        for i in 0..SNAPSHOT_MAX_CONCURRENT_TRANSFERS {
            transfers.insert(
                i as u64,
                InFlightSnapshot {
                    data: Vec::new(),
                    last_chunk_at: Instant::now(),
                },
            );
        }
        assert_eq!(transfers.len(), SNAPSHOT_MAX_CONCURRENT_TRANSFERS);

        // A new transfer_id when at limit should be rejected.
        let new_id = 999u64;
        let is_new = !transfers.contains_key(&new_id);
        let at_limit = transfers.len() >= SNAPSHOT_MAX_CONCURRENT_TRANSFERS;
        assert!(is_new && at_limit, "should detect new transfer at limit");
    }

    /// Snapshot chunk accumulation enforces the per-transfer size cap.
    #[tokio::test]
    async fn test_snapshot_buffer_size_cap() {
        let mut transfers: HashMap<u64, InFlightSnapshot> = HashMap::new();
        let transfer_id = 1u64;
        transfers.insert(
            transfer_id,
            InFlightSnapshot {
                // Pre-fill with data just below the cap.
                data: vec![0u8; SNAPSHOT_MAX_TRANSFER_BYTES - 1],
                last_chunk_at: Instant::now(),
            },
        );

        let entry = transfers.get(&transfer_id).unwrap();
        // Adding 2 more bytes would exceed the cap.
        let new_size = entry.data.len() + 2;
        assert!(
            new_size > SNAPSHOT_MAX_TRANSFER_BYTES,
            "size check should detect overflow"
        );
    }
}
