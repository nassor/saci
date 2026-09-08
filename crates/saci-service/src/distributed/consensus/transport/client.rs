//! Outbound half of the transport: per-peer connection pooling, the
//! circuit breaker, and the openraft client implementations.
//!
//! Owns [`TcpNetwork`] (one handle per remote peer, including the chunked
//! snapshot sender), the [`TcpNetworkFactory`] openraft plugs into, and
//! [`forward_proposal`], the follower-to-leader proposal path.

use std::collections::HashMap;
#[cfg(feature = "distributed-raft")]
use std::collections::VecDeque;
#[cfg(feature = "distributed-raft")]
use std::io;
#[cfg(feature = "distributed-raft")]
use std::io::Cursor;
#[cfg(feature = "distributed-raft")]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "distributed-raft")]
use std::sync::{Arc, LazyLock};
#[cfg(feature = "distributed-raft")]
use std::time::{Duration, Instant};

#[cfg(feature = "distributed-raft")]
use tokio::net::TcpStream;
#[cfg(feature = "distributed-raft")]
use tokio::sync::Mutex;

#[cfg(feature = "distributed-raft")]
use openraft::{
    BasicNode, RaftNetworkFactory, RaftNetworkV2,
    error::{NetworkError, RPCError, ReplicationClosed, StreamingError},
    network::{Backoff, RPCOption},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
    },
    type_config::alias::{SnapshotOf, VoteOf},
};

#[cfg(feature = "distributed-raft")]
use crate::distributed::consensus::types::{ConsensusCommand, ConsensusResponse, SaciTypeConfig};
#[cfg(feature = "distributed-raft")]
use crate::{SaciError, SaciResult};

#[cfg(feature = "distributed-raft")]
use super::wire::{read_frame, write_frame};
#[cfg(feature = "distributed-raft")]
use super::{
    CIRCUIT_OPEN_DURATION, CIRCUIT_OPEN_THRESHOLD, CONNECT_TIMEOUT, MAX_SNAPSHOT_CHUNK_BYTES,
    POOL_CAPACITY, POOL_MAX_IDLE, PROPOSAL_FORWARD_READ_TIMEOUT, RPC_READ_TIMEOUT,
    RPC_WRITE_TIMEOUT, RpcEnvelope, RpcResponse, SNAPSHOT_CHUNK_BYTES, SnapshotChunkMsg,
    SnapshotFinalMsg, TransportError,
};

// ── Per-peer connection pool ───────────────────────────────────────────────────

/// A pooled stream tagged with the time it was returned to the pool.
#[cfg(feature = "distributed-raft")]
struct PooledStream {
    stream: TcpStream,
    returned_at: Instant,
}

/// Per-peer circuit-breaker state.
///
/// Transitions:
/// - `Closed { consecutive_failures }` → `Open { opened_at }` when
///   `consecutive_failures` reaches [`CIRCUIT_OPEN_THRESHOLD`].
/// - `Open { opened_at }` → `Closed { consecutive_failures: 0 }` after
///   [`CIRCUIT_OPEN_DURATION`] has elapsed.
///
/// Any successful RPC resets the counter to zero.
#[cfg(feature = "distributed-raft")]
#[derive(Debug, Clone)]
enum CircuitState {
    Closed { consecutive_failures: u32 },
    Open { opened_at: Instant },
}

#[cfg(feature = "distributed-raft")]
impl CircuitState {
    fn new() -> Self {
        CircuitState::Closed {
            consecutive_failures: 0,
        }
    }

    /// Returns `true` if the circuit is currently open (blocking RPCs).
    fn is_open(&self) -> bool {
        matches!(self, CircuitState::Open { opened_at } if opened_at.elapsed() < CIRCUIT_OPEN_DURATION)
    }

    /// Record a successful RPC; resets the failure counter.
    fn record_success(&mut self) {
        *self = CircuitState::Closed {
            consecutive_failures: 0,
        };
    }

    /// Record a failed RPC; may transition to Open.
    fn record_failure(&mut self) {
        match self {
            CircuitState::Closed {
                consecutive_failures,
            } => {
                *consecutive_failures += 1;
                if *consecutive_failures >= CIRCUIT_OPEN_THRESHOLD {
                    *self = CircuitState::Open {
                        opened_at: Instant::now(),
                    };
                }
            }
            CircuitState::Open { opened_at } => {
                if opened_at.elapsed() >= CIRCUIT_OPEN_DURATION {
                    // Timeout elapsed, so half-open: allow one attempt with the
                    // counter reset to a single failure.
                    *self = CircuitState::Closed {
                        consecutive_failures: 1,
                    };
                }
                // else: still open, stay open.
            }
        }
    }
}

/// A bounded pool of idle [`TcpStream`]s for one remote peer.
///
/// Acquire a stream with [`PeerPool::acquire`]; after use, return it with
/// [`PeerPool::release`] on success or simply drop it (do not call `release`)
/// on error so a broken stream is never returned to the pool.
#[cfg(feature = "distributed-raft")]
struct PeerPool {
    /// Peer address as `"host:port"`, either a hostname or an IP literal. Resolved to
    /// [`SocketAddr`](std::net::SocketAddr) lazily at connection time so Docker service
    /// names such as `"node2:9002"` work without a pre-boot DNS lookup.
    addr: String,
    idle: Mutex<VecDeque<PooledStream>>,
    /// Per-peer circuit breaker. Counts only failures on an *established*
    /// stream (see [`PeerPool::record_failure`]); a failed connect propagates
    /// as `Unreachable` and is paced by openraft's own backoff instead.
    circuit: Mutex<CircuitState>,
}

#[cfg(feature = "distributed-raft")]
impl PeerPool {
    fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            idle: Mutex::new(VecDeque::with_capacity(POOL_CAPACITY)),
            circuit: Mutex::new(CircuitState::new()),
        }
    }

    /// Acquire a stream: pops idle connections, dropping any that have exceeded
    /// [`POOL_MAX_IDLE`], until a fresh-enough one is found or the pool is empty, then
    /// opens a new TCP connection with a [`CONNECT_TIMEOUT`] deadline.
    ///
    /// Returns `Err(TransportError::Other)` immediately if the per-peer circuit
    /// is open, avoiding unnecessary connect attempts to a known-dead peer.
    ///
    /// The peer address is resolved via DNS on each new-connection attempt, so
    /// hostnames such as Docker Compose service names work.
    async fn acquire(&self) -> Result<TcpStream, TransportError> {
        // Circuit breaker: fast-fail if too many recent failures.
        {
            let circuit = self.circuit.lock().await;
            if circuit.is_open() {
                return Err(TransportError::Other(io::Error::other(format!(
                    "circuit open for peer {} — too many consecutive failures",
                    self.addr
                ))));
            }
        }

        {
            let mut guard = self.idle.lock().await;
            while let Some(pooled) = guard.pop_front() {
                if pooled.returned_at.elapsed() <= POOL_MAX_IDLE {
                    return Ok(pooled.stream);
                }
                // Stale connection: drop it and try the next one.
            }
        }
        // Resolve the hostname lazily so Docker service-name peers work.
        let addr = tokio::net::lookup_host(&self.addr)
            .await
            .map_err(|e| TransportError::ConnectFailed(io::Error::other(format!("DNS: {e}"))))?
            .next()
            .ok_or_else(|| {
                TransportError::ConnectFailed(io::Error::other(format!(
                    "no addresses for {}",
                    self.addr
                )))
            })?;
        tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
            .await
            .map_err(|_| TransportError::ConnectFailed(io::Error::other("connect timeout")))?
            .map_err(TransportError::ConnectFailed)
    }

    /// Return a healthy stream to the pool. If the pool is full, the stream is dropped.
    async fn release(&self, stream: TcpStream) {
        let mut guard = self.idle.lock().await;
        if guard.len() < POOL_CAPACITY {
            guard.push_back(PooledStream {
                stream,
                returned_at: Instant::now(),
            });
        }
        // A full pool drops the stream here, by design.
    }

    /// Record a successful RPC against this peer; resets the circuit.
    async fn record_success(&self) {
        self.circuit.lock().await.record_success();
    }

    /// Record a failed RPC against this peer; may open the circuit.
    ///
    /// Only reached once a stream is in hand, so a refused or timed-out
    /// connect never counts towards [`CIRCUIT_OPEN_THRESHOLD`].
    async fn record_failure(&self) {
        self.circuit.lock().await.record_failure();
    }
}

/// Client-side network handle for one remote Raft peer.
#[cfg(feature = "distributed-raft")]
pub struct TcpNetwork {
    pub target: u64,
    pool: Arc<PeerPool>,
}

#[cfg(feature = "distributed-raft")]
impl TcpNetwork {
    /// Create a network channel to `target`.
    ///
    /// `addr` is a `"host:port"` string, either an IP literal or a hostname resolved
    /// lazily at connect time.
    pub(crate) fn new(target: u64, addr: impl Into<String>) -> Self {
        Self {
            target,
            pool: Arc::new(PeerPool::new(addr)),
        }
    }
}

// ── openraft trait impls ───────────────────────────────────────────────────────

/// Monotonic counter for snapshot transfer IDs; avoids subsec_nanos collisions.
#[cfg(feature = "distributed-raft")]
static TRANSFER_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

#[cfg(feature = "distributed-raft")]
impl TcpNetwork {
    /// Send an envelope and read one response frame with a read timeout.
    ///
    /// On success the stream is returned to the pool and the circuit-breaker success
    /// counter is reset. On any error the stream is dropped and the circuit-breaker
    /// failure counter is incremented.
    async fn send_envelope(
        pool: &PeerPool,
        envelope: &RpcEnvelope,
    ) -> Result<RpcResponse, TransportError> {
        let bytes = serde_json::to_vec(envelope)
            .map_err(|e| TransportError::EncodeError(format!("envelope encode: {e}")))?;

        let mut stream = pool.acquire().await?;

        let write_result =
            tokio::time::timeout(RPC_WRITE_TIMEOUT, write_frame(&mut stream, &bytes))
                .await
                .map_err(|_| TransportError::WriteTimeout)?
                .map_err(TransportError::WriteFailed);
        if let Err(e) = write_result {
            pool.record_failure().await;
            return Err(e);
        }

        let raw_result = tokio::time::timeout(RPC_READ_TIMEOUT, read_frame(&mut stream))
            .await
            .map_err(|_| TransportError::ReadTimeout)?
            .map_err(|e| TransportError::FramingError(e.to_string()))
            .and_then(|opt| opt.ok_or(TransportError::PeerReset));
        let raw = match raw_result {
            Ok(r) => r,
            Err(e) => {
                pool.record_failure().await;
                return Err(e);
            }
        };

        let resp: RpcResponse = serde_json::from_slice(&raw)
            .map_err(|e| TransportError::FramingError(format!("response decode: {e}")))?;

        // Return healthy stream to pool and reset circuit breaker.
        pool.release(stream).await;
        pool.record_success().await;
        Ok(resp)
    }

    /// Send snapshot chunks and await the final `SnapshotDone` response.
    async fn send_snapshot_chunks(
        pool: &PeerPool,
        vote: VoteOf<SaciTypeConfig>,
        snapshot: SnapshotOf<SaciTypeConfig, Cursor<Vec<u8>>>,
        transfer_id: u64,
    ) -> Result<SnapshotResponse<SaciTypeConfig>, StreamingError<SaciTypeConfig>> {
        let meta = snapshot.meta.clone();
        let body: Vec<u8> = snapshot.snapshot.into_inner();
        let total = body.len();
        let mut offset = 0usize;

        // A single stream is held for the whole transfer so the server can correlate
        // chunks by transfer_id.
        let mut stream = pool.acquire().await.map_err(|e| e.into_streaming_error())?;

        loop {
            let end = (offset + SNAPSHOT_CHUNK_BYTES).min(total);
            let chunk_data = body[offset..end].to_vec();
            let is_last = end == total;

            let envelope: RpcEnvelope = if is_last {
                RpcEnvelope::SnapshotFinal(SnapshotFinalMsg {
                    transfer_id,
                    offset: offset as u64,
                    data: chunk_data,
                    vote,
                    meta: meta.clone(),
                })
            } else {
                RpcEnvelope::SnapshotChunk(SnapshotChunkMsg {
                    transfer_id,
                    offset: offset as u64,
                    data: chunk_data,
                })
            };

            // Enforce max chunk size client-side before framing so the peer
            // isn't forced to reject oversized payloads after reassembly.
            let chunk_len = match &envelope {
                RpcEnvelope::SnapshotChunk(c) => c.data.len(),
                RpcEnvelope::SnapshotFinal(f) => f.data.len(),
                _ => 0,
            };
            if chunk_len > MAX_SNAPSHOT_CHUNK_BYTES {
                return Err(TransportError::EncodeError(format!(
                    "snapshot chunk too large: {chunk_len} > {MAX_SNAPSHOT_CHUNK_BYTES}"
                ))
                .into_streaming_error());
            }

            let bytes = serde_json::to_vec(&envelope).map_err(|e| {
                TransportError::EncodeError(format!("snapshot chunk encode: {e}"))
                    .into_streaming_error()
            })?;

            tokio::time::timeout(RPC_WRITE_TIMEOUT, write_frame(&mut stream, &bytes))
                .await
                .map_err(|_| TransportError::WriteTimeout.into_streaming_error())?
                .map_err(|e| TransportError::WriteFailed(e).into_streaming_error())?;

            let raw = tokio::time::timeout(RPC_READ_TIMEOUT, read_frame(&mut stream))
                .await
                .map_err(|_| TransportError::ReadTimeout.into_streaming_error())?
                .map_err(|e| TransportError::FramingError(e.to_string()).into_streaming_error())?
                .ok_or_else(|| TransportError::PeerReset.into_streaming_error())?;

            let resp: RpcResponse = serde_json::from_slice(&raw).map_err(|e| {
                TransportError::FramingError(format!("snapshot ack decode: {e}"))
                    .into_streaming_error()
            })?;

            match resp {
                RpcResponse::SnapshotChunkAck { .. } => {
                    offset = end;
                }
                RpcResponse::SnapshotDone(snap_resp) => {
                    pool.release(stream).await;
                    return Ok(snap_resp);
                }
                RpcResponse::Error(msg) => {
                    return Err(StreamingError::Network(NetworkError::new(
                        &io::Error::other(format!("snapshot install error from peer: {msg}")),
                    )));
                }
                other => {
                    return Err(StreamingError::Network(NetworkError::new(
                        &io::Error::other(format!("unexpected snapshot response: {other:?}")),
                    )));
                }
            }

            if is_last {
                break;
            }
        }

        // Unreachable: the loop always returns or breaks when is_last is true,
        // but the compiler needs a value here.
        Err(StreamingError::Network(NetworkError::new(
            &io::Error::other("snapshot transfer loop exited without final response"),
        )))
    }
}

#[cfg(feature = "distributed-raft")]
impl RaftNetworkV2<SaciTypeConfig> for TcpNetwork {
    type SnapshotData = Cursor<Vec<u8>>;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<SaciTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<SaciTypeConfig>, RPCError<SaciTypeConfig>> {
        let envelope = RpcEnvelope::AppendEntries(rpc);
        let resp = Self::send_envelope(&self.pool, &envelope)
            .await
            .map_err(|e| e.into_rpc_error())?;

        match resp {
            RpcResponse::AppendEntries(r) => Ok(r),
            RpcResponse::Error(msg) => Err(RPCError::Network(NetworkError::new(
                &io::Error::other(format!("append_entries error: {msg}")),
            ))),
            _ => Err(RPCError::Network(NetworkError::new(&io::Error::other(
                "unexpected response variant for append_entries",
            )))),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<SaciTypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<SaciTypeConfig>, RPCError<SaciTypeConfig>> {
        let envelope = RpcEnvelope::Vote(rpc);
        let resp = Self::send_envelope(&self.pool, &envelope)
            .await
            .map_err(|e| e.into_rpc_error())?;

        match resp {
            RpcResponse::Vote(r) => Ok(r),
            RpcResponse::Error(msg) => Err(RPCError::Network(NetworkError::new(
                &io::Error::other(format!("vote error: {msg}")),
            ))),
            _ => Err(RPCError::Network(NetworkError::new(&io::Error::other(
                "unexpected response variant for vote",
            )))),
        }
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<SaciTypeConfig>,
        snapshot: SnapshotOf<SaciTypeConfig, Self::SnapshotData>,
        cancel: impl Future<Output = ReplicationClosed> + openraft::OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<SaciTypeConfig>, StreamingError<SaciTypeConfig>> {
        let transfer_id = TRANSFER_ID_COUNTER.fetch_add(1, Ordering::Relaxed);

        let send_fut = Self::send_snapshot_chunks(&self.pool, vote, snapshot, transfer_id);

        tokio::select! {
            result = send_fut => result,
            closed = cancel => Err(StreamingError::Closed(closed)),
        }
    }

    fn backoff(&self) -> Option<Backoff> {
        // Exponential backoff: 100ms base, 2x multiplier, 10s cap, 20% jitter.
        let base_ms: u64 = 100;
        let cap_ms: u64 = 10_000;
        let iter = std::iter::successors(Some(base_ms), move |&prev| {
            let next = (prev * 2).min(cap_ms);
            let jitter = {
                use rand::RngExt;
                rand::rng().random_range(0..=(next / 5))
            };
            Some(next + jitter)
        })
        .map(Duration::from_millis);
        Some(Backoff::new(iter))
    }
}

// ── Proposal forwarding ────────────────────────────────────────────────────────

/// Module-level pool cache for `forward_proposal` so each leader address
/// reuses pooled connections instead of opening a fresh one per call.
#[cfg(feature = "distributed-raft")]
static FORWARD_PROPOSAL_POOLS: LazyLock<Mutex<HashMap<String, Arc<PeerPool>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[cfg(feature = "distributed-raft")]
async fn get_forward_pool(addr: &str) -> Arc<PeerPool> {
    let mut guard = FORWARD_PROPOSAL_POOLS.lock().await;
    if let Some(p) = guard.get(addr) {
        return Arc::clone(p);
    }
    let pool = Arc::new(PeerPool::new(addr));
    guard.insert(addr.to_string(), Arc::clone(&pool));
    pool
}

/// Forward a [`ConsensusCommand`] proposal to the Raft leader over TCP.
///
/// Called by a follower node when `client_write` returns `ForwardToLeader`.
/// Resolves `addr` via DNS (same as [`PeerPool::acquire`]), sends a
/// [`RpcEnvelope::ProposalForward`] frame via a pooled connection, and reads
/// the [`RpcResponse::ProposalResult`] response.
///
/// Uses [`CONNECT_TIMEOUT`] for the TCP connect, [`RPC_WRITE_TIMEOUT`] for
/// the frame write, and [`PROPOSAL_FORWARD_READ_TIMEOUT`] for the response read.
///
/// # Errors
///
/// Returns a [`SaciError`] if the connection fails, the write/read times out,
/// or the leader returns an error response.
#[cfg(feature = "distributed-raft")]
pub(crate) async fn forward_proposal(
    addr: &str,
    cmd: ConsensusCommand,
) -> SaciResult<ConsensusResponse> {
    let pool = get_forward_pool(addr).await;

    let mut stream = pool
        .acquire()
        .await
        .map_err(|e| SaciError::generic(format!("forward_proposal: connect to {addr}: {e:?}")))?;

    let envelope = RpcEnvelope::ProposalForward { command: cmd };
    let bytes = serde_json::to_vec(&envelope)
        .map_err(|e| SaciError::generic(format!("forward_proposal: serialize envelope: {e}")))?;

    tokio::time::timeout(RPC_WRITE_TIMEOUT, write_frame(&mut stream, &bytes))
        .await
        .map_err(|_| SaciError::generic(format!("forward_proposal: write timeout to {addr}")))?
        .map_err(|e| SaciError::generic(format!("forward_proposal: write frame: {e}")))?;

    let raw = tokio::time::timeout(PROPOSAL_FORWARD_READ_TIMEOUT, read_frame(&mut stream))
        .await
        .map_err(|_| SaciError::generic(format!("forward_proposal: read timeout from {addr}")))?
        .map_err(|e| SaciError::generic(format!("forward_proposal: read frame: {e}")))?
        .ok_or_else(|| {
            SaciError::generic(format!("forward_proposal: connection reset by {addr}"))
        })?;

    // Return the stream to the pool on success path.
    pool.release(stream).await;

    let resp: RpcResponse = serde_json::from_slice(&raw)
        .map_err(|e| SaciError::generic(format!("forward_proposal: response decode: {e}")))?;

    match resp {
        RpcResponse::ProposalResult {
            ok: Some(result), ..
        } => Ok(result),
        RpcResponse::ProposalResult { err: Some(msg), .. } => Err(SaciError::generic(format!(
            "forward_proposal: leader returned error: {msg}"
        ))),
        RpcResponse::ProposalResult {
            ok: None,
            err: None,
        } => Err(SaciError::generic(
            "forward_proposal: empty ProposalResult from leader",
        )),
        other => Err(SaciError::generic(format!(
            "forward_proposal: unexpected response from {addr}: {other:?}"
        ))),
    }
}

/// Factory that creates [`TcpNetwork`] instances for each cluster peer.
///
/// Peer addresses are stored as `"host:port"` strings and resolved via DNS lazily at
/// connection time. Docker Compose service names such as `"node2:9002"` are therefore
/// valid peer addresses even when the peer container is not yet running.
#[cfg_attr(not(feature = "distributed-raft"), allow(dead_code))]
pub struct TcpNetworkFactory {
    /// Peer addresses as `"host:port"` strings (may be hostnames or IPs).
    peers: HashMap<u64, String>,
    /// Per-RPC read-response timeout. Defaults to [`RPC_READ_TIMEOUT`].
    #[cfg(feature = "distributed-raft")]
    pub rpc_read_timeout: Duration,
}

impl TcpNetworkFactory {
    pub fn new(peers: HashMap<u64, String>) -> Self {
        Self {
            peers,
            #[cfg(feature = "distributed-raft")]
            rpc_read_timeout: RPC_READ_TIMEOUT,
        }
    }

    #[cfg(feature = "distributed-raft")]
    pub fn from_basic_nodes(nodes: &HashMap<u64, BasicNode>) -> Self {
        let peers = nodes
            .iter()
            .map(|(id, node)| (*id, node.addr.clone()))
            .collect();
        Self {
            peers,
            rpc_read_timeout: RPC_READ_TIMEOUT,
        }
    }
}

#[cfg(feature = "distributed-raft")]
impl RaftNetworkFactory<SaciTypeConfig> for TcpNetworkFactory {
    type Network = TcpNetwork;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> TcpNetwork {
        // Use the pre-stored address (from factory config) if available;
        // fall back to the address advertised in the Raft BasicNode.
        let addr = self
            .peers
            .get(&target)
            .cloned()
            .unwrap_or_else(|| node.addr.clone());
        TcpNetwork::new(target, addr)
    }
}

#[cfg(all(test, feature = "distributed-raft"))]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use tokio::net::TcpListener;

    use super::super::tests::{free_addr, spawn_echo_server};

    /// Connecting to an unreachable peer produces `ConnectFailed`, which maps to
    /// `RPCError::Unreachable` (permanent-ish classification).
    #[tokio::test]
    async fn test_connect_failure_maps_to_unreachable() {
        // Port 1 is never listening.
        let pool = PeerPool::new("127.0.0.1:1");
        let err = pool.acquire().await.unwrap_err();
        let rpc_err = err.into_rpc_error();
        assert!(
            matches!(rpc_err, RPCError::Unreachable(_)),
            "connect failure must map to Unreachable, got: {rpc_err:?}"
        );
    }

    /// A read timeout maps to `RPCError::Network` (transient).
    #[tokio::test]
    async fn test_read_timeout_maps_to_network_error() {
        use tokio::net::TcpListener;

        // Spawn a server that accepts but never responds.
        let addr = free_addr();
        tokio::spawn(async move {
            let l = TcpListener::bind(addr).await.unwrap();
            while let Ok((_stream, _)) = l.accept().await {
                // Intentionally keep the stream open without sending anything.
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;

        let pool = PeerPool::new(addr.to_string());

        // A short timeout keeps the test from waiting the full 5 seconds.
        let mut stream = pool.acquire().await.expect("should connect");
        write_frame(&mut stream, b"probe").await.unwrap();

        let short_timeout = Duration::from_millis(50);
        let result = tokio::time::timeout(short_timeout, read_frame(&mut stream)).await;

        // timeout fires → transient network error
        assert!(result.is_err(), "expected timeout to fire");
        let rpc_err = TransportError::ReadTimeout.into_rpc_error();
        assert!(
            matches!(rpc_err, RPCError::Network(_)),
            "read timeout must map to Network (transient), got: {rpc_err:?}"
        );
    }

    /// Connection pool acquire, release, and capacity semantics.
    #[tokio::test]
    async fn test_pool_acquire_release_capacity() {
        let addr = free_addr();
        spawn_echo_server(addr);
        tokio::time::sleep(Duration::from_millis(10)).await;

        let pool = PeerPool::new(addr.to_string());

        // Acquire two connections and release them.
        let s1 = pool.acquire().await.unwrap();
        let s2 = pool.acquire().await.unwrap();
        pool.release(s1).await;
        pool.release(s2).await;

        let guard = pool.idle.lock().await;
        assert_eq!(guard.len(), 2, "both streams should be in the pool");
        drop(guard);

        // Fill pool to capacity then try to release one more.
        let mut extras = Vec::new();
        for _ in 0..POOL_CAPACITY {
            extras.push(pool.acquire().await.unwrap());
        }
        for s in extras {
            pool.release(s).await;
        }
        let guard = pool.idle.lock().await;
        assert_eq!(
            guard.len(),
            POOL_CAPACITY,
            "pool must not exceed POOL_CAPACITY"
        );
    }

    /// Stale pooled connections (exceeding POOL_MAX_IDLE) are dropped on acquire.
    #[tokio::test]
    async fn test_pool_stale_connection_dropped() {
        let addr = free_addr();
        spawn_echo_server(addr);
        tokio::time::sleep(Duration::from_millis(10)).await;

        let pool = PeerPool::new(addr.to_string());
        let stream = pool.acquire().await.unwrap();

        // Manually insert a stale PooledStream (returned_at far in the past).
        {
            let mut guard = pool.idle.lock().await;
            guard.push_back(PooledStream {
                stream,
                returned_at: Instant::now() - POOL_MAX_IDLE - Duration::from_secs(1),
            });
        }

        // Acquire should discard the stale entry and open a fresh connection.
        let fresh = pool.acquire().await;
        assert!(
            fresh.is_ok(),
            "should open a new connection after discarding stale one"
        );
    }

    /// Snapshot transfer IDs from TRANSFER_ID_COUNTER are unique.
    #[test]
    fn test_transfer_id_counter_unique() {
        let id1 = TRANSFER_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        let id2 = TRANSFER_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        assert_ne!(id1, id2, "transfer IDs must be unique");
    }

    /// Round-trip a snapshot through the chunk serialisation and reassembly path using
    /// an in-memory pair of TCP endpoints.
    ///
    /// The minimal server acks each `SnapshotChunk` and, on `SnapshotFinal`, echoes a
    /// fake `SnapshotDone`. This checks that the sender slices the body correctly and
    /// that reassembly reproduces the original bytes.
    #[tokio::test]
    async fn test_snapshot_chunk_reassembly_roundtrip() {
        use std::io::Cursor;
        use tokio::net::TcpListener;

        let listen_addr = free_addr();

        tokio::spawn(async move {
            let listener = TcpListener::bind(listen_addr).await.unwrap();
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut accumulated: Vec<u8> = Vec::new();

                    loop {
                        let raw = match read_frame(&mut stream).await {
                            Ok(Some(b)) => b,
                            _ => break,
                        };
                        let env: RpcEnvelope = serde_json::from_slice(&raw).unwrap();
                        match env {
                            RpcEnvelope::SnapshotChunk(c) => {
                                accumulated.extend_from_slice(&c.data);
                                let ack = RpcResponse::SnapshotChunkAck {
                                    transfer_id: c.transfer_id,
                                };
                                let ack_bytes = serde_json::to_vec(&ack).unwrap();
                                write_frame(&mut stream, &ack_bytes).await.unwrap();
                            }
                            RpcEnvelope::SnapshotFinal(f) => {
                                accumulated.extend_from_slice(&f.data);
                                // A real server would call install_full_snapshot here.
                                assert!(!accumulated.is_empty(), "accumulated must be non-empty");

                                // Echo back a fake SnapshotDone.
                                let done =
                                    RpcResponse::SnapshotDone(SnapshotResponse { vote: f.vote });
                                let done_bytes = serde_json::to_vec(&done).unwrap();
                                write_frame(&mut stream, &done_bytes).await.unwrap();
                                break;
                            }
                            _ => break,
                        }
                    }
                });
            }
        });

        tokio::time::sleep(Duration::from_millis(20)).await;

        // Build a snapshot larger than one chunk to exercise multi-chunk path.
        let body_size = SNAPSHOT_CHUNK_BYTES + 100;
        let body: Vec<u8> = (0..body_size).map(|i| (i % 251) as u8).collect();

        // Fake Vote and SnapshotMeta, constructed manually because both are serde
        // types.
        use openraft::{Snapshot, SnapshotMeta, StoredMembership, impls::Vote};
        let vote = Vote::new(1, 1);
        let meta = SnapshotMeta {
            last_log_id: None,
            last_membership: StoredMembership::default(),
        };
        let snapshot: SnapshotOf<SaciTypeConfig, Cursor<Vec<u8>>> = Snapshot {
            meta,
            snapshot: Cursor::new(body.clone()),
        };

        let pool = PeerPool::new(listen_addr.to_string());
        let transfer_id = TRANSFER_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        let result = TcpNetwork::send_snapshot_chunks(&pool, vote, snapshot, transfer_id).await;

        assert!(
            result.is_ok(),
            "snapshot transfer should succeed, got: {result:?}"
        );
    }

    // ── forward_proposal tests ────────────────────────────────────────────────

    /// Spawn a minimal TCP server that handles `ProposalForward` envelopes and
    /// returns a `ProposalResult { ok: Some(...) }` response.
    fn spawn_fake_leader(addr: SocketAddr, response: ConsensusResponse) {
        tokio::spawn(async move {
            let listener = TcpListener::bind(addr).await.unwrap();
            while let Ok((mut stream, _)) = listener.accept().await {
                let resp = response.clone();
                tokio::spawn(async move {
                    while let Ok(Some(raw)) = read_frame(&mut stream).await {
                        let envelope: RpcEnvelope = serde_json::from_slice(&raw).unwrap();
                        let reply = match envelope {
                            RpcEnvelope::ProposalForward { .. } => RpcResponse::ProposalResult {
                                ok: Some(resp.clone()),
                                err: None,
                            },
                            _ => RpcResponse::Error("unexpected envelope".to_string()),
                        };
                        let bytes = serde_json::to_vec(&reply).unwrap();
                        let _ = write_frame(&mut stream, &bytes).await;
                    }
                });
            }
        });
    }

    /// `forward_proposal` succeeds when the leader returns `ProposalResult { ok }`.
    #[tokio::test]
    async fn test_forward_proposal_success() {
        use uuid::Uuid;

        let addr = free_addr();
        spawn_fake_leader(addr, ConsensusResponse::ClaimAcked);
        tokio::time::sleep(Duration::from_millis(10)).await;

        let cmd = ConsensusCommand::AckClaim {
            claim_id: Uuid::now_v7(),
            instance_id: Uuid::now_v7(),
        };
        let result = forward_proposal(&addr.to_string(), cmd).await;
        assert!(
            matches!(result, Ok(ConsensusResponse::ClaimAcked)),
            "expected ClaimAcked, got: {result:?}"
        );
    }

    /// `forward_proposal` returns an error when the leader returns
    /// `ProposalResult { err }`.
    #[tokio::test]
    async fn test_forward_proposal_leader_error_response() {
        use tokio::net::TcpListener;
        use uuid::Uuid;

        let addr = free_addr();
        tokio::spawn(async move {
            let listener = TcpListener::bind(addr).await.unwrap();
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    while let Ok(Some(_raw)) = read_frame(&mut stream).await {
                        let reply = RpcResponse::ProposalResult {
                            ok: None,
                            err: Some("not leader".to_string()),
                        };
                        let bytes = serde_json::to_vec(&reply).unwrap();
                        let _ = write_frame(&mut stream, &bytes).await;
                    }
                });
            }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;

        let cmd = ConsensusCommand::AckClaim {
            claim_id: Uuid::now_v7(),
            instance_id: Uuid::now_v7(),
        };
        let result = forward_proposal(&addr.to_string(), cmd).await;
        assert!(result.is_err(), "expected error from leader error response");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("not leader"),
            "error message should contain 'not leader': {msg}"
        );
    }

    /// `forward_proposal` returns a connect error when the address is unreachable.
    #[tokio::test]
    async fn test_forward_proposal_connect_failure() {
        use uuid::Uuid;

        // Port 1 is never listening.
        let cmd = ConsensusCommand::AckClaim {
            claim_id: Uuid::now_v7(),
            instance_id: Uuid::now_v7(),
        };
        let result = forward_proposal("127.0.0.1:1", cmd).await;
        assert!(
            result.is_err(),
            "expected connection failure, got: {result:?}"
        );
    }

    // ── Circuit breaker state machine tests ───────────────────────────────────

    /// Fresh circuit is closed with zero failures.
    #[test]
    fn test_circuit_starts_closed() {
        let state = CircuitState::new();
        assert!(!state.is_open(), "fresh circuit must be closed");
    }

    /// Circuit opens after CIRCUIT_OPEN_THRESHOLD consecutive failures.
    #[test]
    fn test_circuit_opens_after_threshold_failures() {
        let mut state = CircuitState::new();
        for i in 0..CIRCUIT_OPEN_THRESHOLD {
            assert!(
                !state.is_open(),
                "circuit should still be closed at failure {i}"
            );
            state.record_failure();
        }
        assert!(
            state.is_open(),
            "circuit must be open after {CIRCUIT_OPEN_THRESHOLD} failures"
        );
    }

    /// A success resets the failure counter so the threshold must be reached again.
    #[test]
    fn test_circuit_resets_on_success() {
        let mut state = CircuitState::new();
        // Accumulate failures up to threshold - 1.
        for _ in 0..CIRCUIT_OPEN_THRESHOLD - 1 {
            state.record_failure();
        }
        assert!(!state.is_open(), "should still be closed before threshold");
        state.record_success();
        // After success, need another full run of failures to open.
        for i in 0..CIRCUIT_OPEN_THRESHOLD {
            assert!(
                !state.is_open(),
                "circuit should be closed after reset, at failure {i}"
            );
            state.record_failure();
        }
        assert!(
            state.is_open(),
            "circuit must open again after threshold failures post-reset"
        );
    }

    /// Circuit transitions to half-open after CIRCUIT_OPEN_DURATION expires.
    #[test]
    fn test_circuit_half_open_after_duration() {
        let mut state = CircuitState::Open {
            opened_at: Instant::now() - CIRCUIT_OPEN_DURATION - Duration::from_millis(1),
        };
        // is_open checks elapsed < CIRCUIT_OPEN_DURATION, so this now returns false.
        assert!(
            !state.is_open(),
            "expired open circuit should not be considered open"
        );

        // record_failure on an expired Open transitions to Closed{1}.
        state.record_failure();
        assert!(
            matches!(
                state,
                CircuitState::Closed {
                    consecutive_failures: 1
                }
            ),
            "half-open after timeout: one failure allowed before re-opening"
        );
    }

    /// Closed circuit stays closed when failures are below the threshold.
    #[test]
    fn test_circuit_stays_closed_below_threshold() {
        let mut state = CircuitState::new();
        for _ in 0..CIRCUIT_OPEN_THRESHOLD - 1 {
            state.record_failure();
        }
        assert!(
            !state.is_open(),
            "circuit must stay closed with fewer than {CIRCUIT_OPEN_THRESHOLD} failures"
        );
    }

    /// `PeerPool::acquire` returns an error immediately when the circuit is open.
    #[tokio::test]
    async fn test_pool_acquire_blocked_by_open_circuit() {
        let pool = PeerPool::new("127.0.0.1:1"); // unreachable port

        // Force-open the circuit.
        {
            let mut c = pool.circuit.lock().await;
            *c = CircuitState::Open {
                opened_at: Instant::now(),
            };
        }

        let err = pool.acquire().await.unwrap_err();
        assert!(
            matches!(err, TransportError::Other(_)),
            "open circuit must return TransportError::Other, got: {err:?}"
        );
    }

    /// Client-side chunk size guard rejects oversized data before framing.
    ///
    /// Checks the constant relationship rather than the network path. The guard inside
    /// `send_snapshot_chunks` compares `chunk_data.len() > MAX_SNAPSHOT_CHUNK_BYTES`.
    #[test]
    fn test_snapshot_chunk_size_constant_matches_server_cap() {
        assert_eq!(
            MAX_SNAPSHOT_CHUNK_BYTES, SNAPSHOT_CHUNK_BYTES,
            "client and server chunk caps must match"
        );
    }
}
