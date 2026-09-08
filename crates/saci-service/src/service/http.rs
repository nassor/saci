//! HTTP control-plane for the SACI service.
//!
//! Five endpoints, always present:
//!
//! | Endpoint    | Purpose |
//! |-------------|---------|
//! | `GET /`        | Redirect to the dashboard, or the list of mounted endpoints |
//! | `GET /health`  | Liveness probe: checks watchdog counter freshness |
//! | `GET /ready`   | Readiness probe: checks the `ready` flag |
//! | `GET /metrics` | Prometheus metrics exposition |
//! | `GET /status`  | Rich JSON status including cluster/standalone stats |
//!
//! Plus, when the inspector is enabled, the JSON API and dashboard from
//! [`inspector_api`](super::inspector_api), and when standalone-mode lifecycle
//! control is enabled, the workflow endpoints from
//! [`lifecycle_api`](super::lifecycle_api).
//!
//! ## Usage
//!
//! ```rust,no_run
//! # #[cfg(feature = "service")]
//! # {
//! use std::sync::{Arc, atomic::{AtomicBool, AtomicU64}};
//! use std::time::Instant;
//! use saci_service::service::http::{ServiceState, ServiceModeLabel, build_router, serve_http,
//!                            spawn_watchdog};
//! use saci_service::service::config::HttpConfig;
//! use tokio_util::sync::CancellationToken;
//!
//! #[tokio::main]
//! async fn main() {
//!     let registry = Arc::new(prometheus::Registry::new());
//!     let otel_exporter = opentelemetry_prometheus::exporter()
//!         .without_counter_suffixes()
//!         .with_registry((*registry).clone())
//!         .build()
//!         .expect("build otel exporter");
//!     let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
//!         .with_reader(otel_exporter)
//!         .build();
//!     opentelemetry::global::set_meter_provider(provider);
//!
//!     saci_service::metrics::init();
//!
//!     let state = ServiceState {
//!         node_id: 1,
//!         node_name: Some("worker-a".to_string()),
//!         mode: ServiceModeLabel::Standalone,
//!         started_at: Instant::now(),
//!         prometheus_registry: registry,
//!         liveness: Arc::new(AtomicU64::new(0)),
//!         ready: Arc::new(AtomicBool::new(false)),
//!         cluster_probe: None,
//!         standalone_stats: None,
//!         inspector: None,
//!         lifecycle: None,
//!         dlq: None,
//!     };
//!
//!     let cancel = CancellationToken::new();
//!     let _watchdog = spawn_watchdog(state.clone(), cancel.child_token());
//!
//!     let cfg = HttpConfig { bind: "127.0.0.1:8080".to_string(), disabled: false, control: true };
//!     serve_http(&cfg, state, cancel).await.unwrap();
//! }
//! # }
//! ```

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use axum::{
    Json, Router,
    extract::State,
    http::{StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Redirect, Response},
    routing::get,
};
use prometheus::{Registry, TextEncoder};
use serde::Serialize;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

use crate::error::{SaciError, SaciResult};
use crate::service::config::HttpConfig;
use crate::service::standalone::StandaloneStats;

/// A snapshot of cluster state at a point in time.
///
/// Populated by the cluster runner and handed into [`ServiceState`].
/// Fields mirror the Raft state machine and batch-processing counters.
#[derive(Debug, Clone, Serialize)]
pub struct ClusterProbeSnapshot {
    /// Raft role: `"leader"`, `"follower"`, `"candidate"`, or `"learner"`.
    pub role: &'static str,
    /// Overall cluster health: `"healthy"`, `"degraded"`, or `"offline"`.
    pub cluster_health: &'static str,
    /// Node ID of the current Raft leader, if known.
    pub current_leader: Option<u64>,
    /// Current Raft term.
    pub raft_term: u64,
    /// Index of the last log entry.
    pub last_log_index: u64,
    /// Index of the last committed log entry.
    pub commit_index: u64,
    /// Index of the last applied log entry.
    pub last_applied: u64,
    /// Last applied index included in a snapshot (if any).
    pub snapshot_last_applied: Option<u64>,
    /// All cluster members as `(node_id, address)` pairs.
    pub membership: Vec<(u64, String)>,
    /// Node IDs with voting rights.
    pub voters: Vec<u64>,
    /// Non-voting learner node IDs.
    pub learners: Vec<u64>,
    /// Cumulative number of batches processed successfully.
    pub batches_processed_total: u64,
    /// Cumulative number of batch processing failures.
    pub batches_failed_total: u64,
}

/// Abstract cluster-status probe.
///
/// Implemented by the cluster runner and stored in
/// [`ServiceState::cluster_probe`]. Using a trait object means the HTTP layer
/// has no compile-time dependency on `src/distributed/`.
///
/// If the cluster runner is not yet started, `cluster_probe` is `None` and
/// `/status` returns `"cluster": null` even in cluster mode.
pub trait ClusterProbe: Send + Sync {
    /// Capture a point-in-time snapshot of cluster state.
    fn snapshot(&self) -> ClusterProbeSnapshot;
}

/// Which mode the service is running in, as reported by `/status`.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ServiceModeLabel {
    /// Single-node operation, no consensus.
    Standalone,
    /// Multi-node Raft cluster.
    Cluster,
}

/// Shared service state passed into every axum handler via [`State`].
///
/// All fields are cheaply cloneable (atomic refs, `Arc`s, etc.) so axum can
/// clone this per-request without overhead.
#[derive(Clone)]
pub struct ServiceState {
    /// This node's Raft ID (or standalone node ID).
    pub node_id: u64,
    /// Optional human-readable node label.
    pub node_name: Option<String>,
    /// Whether the service runs in standalone or cluster mode.
    pub mode: ServiceModeLabel,
    /// Wall-clock time at which the service started.
    pub started_at: Instant,
    /// Prometheus registry used by `/metrics` to gather and encode metrics.
    pub prometheus_registry: Arc<Registry>,
    /// Monotonic counter incremented every second by the watchdog task.
    ///
    /// The `/health` handler compares this against the expected minimum value
    /// (based on elapsed seconds since start) to detect a wedged service.
    pub liveness: Arc<AtomicU64>,
    /// Set to `true` once the service is ready to serve traffic.
    ///
    /// Standalone: flipped after the first successful pipeline run.
    /// Cluster: flipped after Raft convergence and first successful
    ///   claim (or no-claims-available signal).
    pub ready: Arc<AtomicBool>,
    /// Optional cluster-status probe supplied by the cluster runner.
    pub cluster_probe: Option<Arc<dyn ClusterProbe>>,
    /// Per-workflow standalone statistics supplied by the standalone runner,
    /// keyed by workflow id. `None` in cluster mode.
    pub standalone_stats: Option<Vec<(String, Arc<RwLock<StandaloneStats>>)>>,
    /// The in-process inspector, when telemetry capture is enabled.
    ///
    /// `None` disables `/api/*` and `/ui` entirely: the router does not merge
    /// those routes, so they 404 rather than answering 403 for a feature the
    /// operator switched off.
    pub inspector: Option<crate::inspector::Inspector>,
    /// The workflow lifecycle registry, when the control endpoints are
    /// enabled.
    ///
    /// `None` in cluster mode and under `http { control #false }`: the router
    /// does not merge `/api/workflows*` at all, so those routes 404 rather
    /// than answering 403 for a surface the operator switched off.
    pub lifecycle: Option<Arc<crate::service::lifecycle::LifecycleRegistry>>,
    /// Every declared dead letter queue, when at least one workflow declares
    /// a `dlq` block.
    ///
    /// `None` means the router carries no `/api/dlq` at all, so those routes
    /// 404 rather than answering an empty list for a feature no workflow
    /// asked for.
    pub dlq: Option<Arc<crate::service::dlq::DlqRegistry>>,
}

/// Build the axum [`Router`] with the origin and the five control-plane
/// routes, plus the inspector's JSON API and dashboard, and the workflow
/// lifecycle endpoints, when they are enabled.
///
/// Both optional groups are merged, not gated inside a handler, so a disabled
/// inspector has no `/api/topology` and no `/ui` at all, and a service with no
/// lifecycle registry has no `/api/workflows`.
///
/// The router is not bound to a port here; call [`serve_http`] for that.
pub fn build_router(state: ServiceState) -> Router {
    let mut router = Router::new()
        .route("/", get(handle_root))
        .route("/health", get(handle_health))
        .route("/ready", get(handle_ready))
        .route("/metrics", get(handle_metrics))
        .route("/status", get(handle_status));

    if let Some(inspector) = &state.inspector {
        router = router.merge(crate::service::inspector_api::routes());
        if inspector.ui_enabled() {
            router = router.merge(crate::service::inspector_api::ui_routes());
        }
    }

    if state.lifecycle.is_some() {
        router = router.merge(crate::service::lifecycle_api::routes());
    }

    // The read routes need only a declared queue; a replay runs inside a
    // workflow's runner, so the two `POST`s need the lifecycle plane that
    // knows whether one is live.
    if state.dlq.is_some() {
        router = router.merge(crate::service::dlq_api::read_routes());
        if state.lifecycle.is_some() {
            router = router.merge(crate::service::dlq_api::control_routes());
        }
    }

    router
        .layer(TraceLayer::new_for_http())
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(10),
        ))
        .with_state(state)
}

/// Spawn the liveness watchdog task.
///
/// Increments `state.liveness` by 1 every second. If the main service loop
/// deadlocks the watchdog stops ticking, the `/health` counter goes stale, and
/// the health-check orchestrator restarts the pod.
///
/// The same tick is the single writer for `saci_liveness_counter`, `saci_ready`
/// and `saci_uptime_seconds`.
///
/// The task exits cleanly when `cancel` is cancelled.
pub fn spawn_watchdog(state: ServiceState, cancel: CancellationToken) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let val = state.liveness.fetch_add(1, Ordering::Relaxed) + 1;
                    crate::metrics::instruments().service_gauges(
                        val,
                        state.ready.load(Ordering::Relaxed),
                        state.started_at.elapsed().as_secs_f64(),
                    );
                }
                _ = cancel.cancelled() => break,
            }
        }
        tracing::debug!("watchdog task stopped");
    })
}

// ── Handlers ──────────────────────────────────────────────────────────────────

/// The always-present endpoints, as the plain-text index at `/` lists them.
const PROBE_INDEX: &str = "\
  GET /health   liveness
  GET /ready    readiness
  GET /metrics  Prometheus exposition
  GET /status   node identity, uptime and runner counters
";

/// The inspector's JSON endpoints, listed when the dashboard is off.
const API_INDEX: &str = "\
  GET /api/topology  pipeline graph
  GET /api/snapshot  current counters
  GET /api/traces    recent spans
  GET /api/logs      recent log records
";

/// `GET /`: the bare origin.
///
/// A browser pointed at the address the startup banner prints arrives here.
/// Axum's fallback would answer it with a bodyless 404, which reads as a
/// service that never bound the port, so the origin redirects to the dashboard
/// when one is mounted and names the endpoints that are when it is not.
async fn handle_root(State(state): State<ServiceState>) -> Response {
    let api_mounted = match &state.inspector {
        Some(inspector) if inspector.ui_enabled() => {
            return Redirect::temporary("/ui").into_response();
        }
        Some(_) => true,
        None => false,
    };

    let mut body = String::from("saci-service\n\n");
    body.push_str(PROBE_INDEX);
    if api_mounted {
        body.push_str(API_INDEX);
        body.push_str(
            "\nThe dashboard is off: [observability.inspector] ui = true serves it at /ui.\n",
        );
    } else {
        body.push_str(
            "\nThe inspector is off: [observability.inspector] enabled = true serves the JSON API and the /ui dashboard.\n",
        );
    }

    ([(CONTENT_TYPE, "text/plain; charset=utf-8")], body).into_response()
}

/// How many seconds of liveness-counter lag before `/health` returns 503.
const LIVENESS_STALE_SECONDS: u64 = 5;

/// Response body for `GET /health`.
#[derive(Serialize)]
struct HealthBody {
    status: &'static str,
    uptime_seconds: u64,
    liveness_counter: u64,
}

/// `GET /health`: liveness probe.
///
/// Returns `200 OK` when the watchdog counter has been updated within the last
/// [`LIVENESS_STALE_SECONDS`] seconds, and `503` when the counter is stale,
/// which indicates a potential deadlock in the main loop.
async fn handle_health(State(state): State<ServiceState>) -> impl IntoResponse {
    let uptime_seconds = state.started_at.elapsed().as_secs();
    let liveness_counter = state.liveness.load(Ordering::Relaxed);

    // The watchdog bumps once per second. After N seconds of uptime we expect
    // the counter to be at least (uptime_seconds - LIVENESS_STALE_SECONDS).
    let minimum_expected = uptime_seconds.saturating_sub(LIVENESS_STALE_SECONDS);
    let is_alive = liveness_counter >= minimum_expected;

    let body = HealthBody {
        status: if is_alive { "alive" } else { "stale" },
        uptime_seconds,
        liveness_counter,
    };

    if is_alive {
        (StatusCode::OK, Json(body)).into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response()
    }
}

/// Response body for `GET /ready`.
#[derive(Serialize)]
struct ReadyBody {
    status: &'static str,
}

/// `GET /ready`: readiness probe.
///
/// Returns `200 OK` when the service is ready to handle work, `503` otherwise.
async fn handle_ready(State(state): State<ServiceState>) -> impl IntoResponse {
    if state.ready.load(Ordering::Relaxed) {
        (StatusCode::OK, Json(ReadyBody { status: "ready" })).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ReadyBody {
                status: "not_ready",
            }),
        )
            .into_response()
    }
}

/// `GET /metrics`: Prometheus metrics exposition.
///
/// Returns the full text in Prometheus exposition format 0.0.4.
async fn handle_metrics(State(state): State<ServiceState>) -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let families = state.prometheus_registry.gather();
    let body = encoder.encode_to_string(&families).unwrap_or_default();
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
        .into_response()
}

// ── /status response types ────────────────────────────────────────────────────

#[derive(Serialize)]
struct BuildInfo {
    version: &'static str,
}

#[derive(Serialize)]
struct MemberEntry {
    id: u64,
    addr: String,
}

#[derive(Serialize)]
struct ClusterStatusBody {
    role: &'static str,
    health: &'static str,
    current_leader: Option<u64>,
    raft_term: u64,
    last_log_index: u64,
    commit_index: u64,
    last_applied: u64,
    snapshot_last_applied: Option<u64>,
    membership: Vec<MemberEntry>,
    voters: Vec<u64>,
    learners: Vec<u64>,
    batches_processed_total: u64,
    batches_failed_total: u64,
}

#[derive(Serialize)]
struct StandaloneStatusBody {
    workflow_id: String,
    /// Where the workflow's runner is, when lifecycle control is enabled.
    /// Absent otherwise, so a status document from a service without the
    /// control plane is byte-for-byte what it always was.
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<saci_inspector_wire::WorkflowRunState>,
    iterations: u64,
    rows_processed: u64,
    source_batches_drained: u64,
    sink_batches_written: u64,
    iteration_errors: u64,
    /// Sum of per-item processing time; stream mode only (0 in batch mode).
    total_busy_micros: u64,
    /// Slowest single item; stream mode only (0 in batch mode).
    max_item_micros: u64,
}

#[derive(Serialize)]
struct StatusBody {
    node_id: u64,
    node_name: Option<String>,
    mode: ServiceModeLabel,
    uptime_seconds: u64,
    build: BuildInfo,
    cluster: Option<ClusterStatusBody>,
    standalone: Option<Vec<StandaloneStatusBody>>,
}

/// `GET /status`: rich operational status.
///
/// Returns a JSON document with node identity, uptime, build info, and either
/// cluster or standalone statistics depending on `state.mode`.
async fn handle_status(State(state): State<ServiceState>) -> impl IntoResponse {
    let uptime_seconds = state.started_at.elapsed().as_secs();

    let cluster = if state.mode == ServiceModeLabel::Cluster {
        state.cluster_probe.as_ref().map(|probe| {
            let snap = probe.snapshot();
            ClusterStatusBody {
                role: snap.role,
                health: snap.cluster_health,
                current_leader: snap.current_leader,
                raft_term: snap.raft_term,
                last_log_index: snap.last_log_index,
                commit_index: snap.commit_index,
                last_applied: snap.last_applied,
                snapshot_last_applied: snap.snapshot_last_applied,
                membership: snap
                    .membership
                    .into_iter()
                    .map(|(id, addr)| MemberEntry { id, addr })
                    .collect(),
                voters: snap.voters,
                learners: snap.learners,
                batches_processed_total: snap.batches_processed_total,
                batches_failed_total: snap.batches_failed_total,
            }
        })
    } else {
        None
    };

    let standalone = if state.mode == ServiceModeLabel::Standalone {
        match &state.standalone_stats {
            Some(entries) => {
                let mut body = Vec::with_capacity(entries.len());
                for (workflow_id, stats_lock) in entries {
                    let stats = stats_lock.read().await;
                    body.push(StandaloneStatusBody {
                        workflow_id: workflow_id.clone(),
                        state: state
                            .lifecycle
                            .as_ref()
                            .and_then(|registry| registry.state_of(workflow_id)),
                        iterations: stats.iterations,
                        rows_processed: stats.rows_processed,
                        source_batches_drained: stats.source_batches_drained,
                        sink_batches_written: stats.sink_batches_written,
                        iteration_errors: stats.iteration_errors,
                        total_busy_micros: stats.total_busy_micros,
                        max_item_micros: stats.max_item_micros,
                    });
                }
                Some(body)
            }
            // Standalone mode but the runner has not supplied per-workflow
            // stats yet (e.g. a `ServiceState` built without going through
            // `serve.rs`): report no entries rather than guessing a
            // workflow id.
            None => Some(Vec::new()),
        }
    } else {
        None
    };

    Json(StatusBody {
        node_id: state.node_id,
        node_name: state.node_name.clone(),
        mode: state.mode,
        uptime_seconds,
        build: BuildInfo {
            version: env!("CARGO_PKG_VERSION"),
        },
        cluster,
        standalone,
    })
}

/// Bind and serve the HTTP control plane.
///
/// If `config.disabled` is `true` this function logs a notice and returns
/// immediately without binding any port.
///
/// The server runs until `cancel` is cancelled, at which point axum drains
/// any in-flight requests and exits cleanly.
///
/// # Errors
///
/// Returns [`SaciError::Generic`] if:
/// - `config.bind` is not a valid [`std::net::SocketAddr`].
/// - The OS rejects the `bind` call (e.g. port already in use).
pub async fn serve_http(
    config: &HttpConfig,
    state: ServiceState,
    cancel: CancellationToken,
) -> SaciResult<()> {
    if config.disabled {
        tracing::info!("HTTP control plane disabled");
        return Ok(());
    }

    let addr: std::net::SocketAddr = config.bind.parse().map_err(|e| {
        SaciError::generic(format!("invalid HTTP bind address '{}': {e}", config.bind))
    })?;

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| SaciError::generic(format!("failed to bind HTTP server on {addr}: {e}")))?;

    tracing::info!("HTTP control plane listening on {addr}");

    let router = build_router(state);

    axum::serve(listener, router)
        .with_graceful_shutdown(async move { cancel.cancelled().await })
        .await
        .map_err(|e| SaciError::generic(format!("HTTP server error: {e}")))?;

    tracing::info!("HTTP control plane shut down cleanly");
    Ok(())
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::time::{Duration, Instant};

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn make_prometheus_registry() -> Arc<Registry> {
        Arc::new(Registry::new())
    }

    fn make_state(mode: ServiceModeLabel) -> ServiceState {
        ServiceState {
            node_id: 1,
            node_name: Some("test-node".to_string()),
            mode,
            started_at: Instant::now(),
            prometheus_registry: make_prometheus_registry(),
            liveness: Arc::new(AtomicU64::new(0)),
            ready: Arc::new(AtomicBool::new(false)),
            cluster_probe: None,
            standalone_stats: None,
            inspector: None,
            lifecycle: None,
            dlq: None,
        }
    }

    /// Bind to an ephemeral port and return the address + cancel token + task handle.
    async fn bind_server(
        state: ServiceState,
    ) -> (
        String,
        CancellationToken,
        tokio::task::JoinHandle<SaciResult<()>>,
    ) {
        let cancel = CancellationToken::new();
        // Every test in this module drives the server with a reqwest client,
        // which 0.13 refuses to build without an installed crypto provider.
        crate::service::install_ring_provider();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();

        let state_clone = state.clone();
        let cancel_clone = cancel.clone();

        let handle = tokio::spawn(async move {
            let router = build_router(state_clone);
            axum::serve(listener, router)
                .with_graceful_shutdown(async move { cancel_clone.cancelled().await })
                .await
                .map_err(|e| SaciError::generic(format!("HTTP server error: {e}")))
        });

        // Give the server a moment to start.
        tokio::time::sleep(Duration::from_millis(50)).await;

        (addr, cancel, handle)
    }

    #[tokio::test]
    async fn test_root_redirects_to_ui_when_inspector_enabled() {
        let mut state = make_state(ServiceModeLabel::Standalone);
        state.inspector = Some(crate::inspector::Inspector::new(
            &crate::inspector::InspectorConfig::default(),
        ));

        let (addr, cancel, _handle) = bind_server(state).await;
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let resp = client.get(format!("http://{addr}/")).send().await.unwrap();
        assert_eq!(resp.status(), 307);
        assert_eq!(resp.headers().get("location").unwrap(), "/ui");

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_root_lists_probe_endpoints_when_inspector_disabled() {
        let state = make_state(ServiceModeLabel::Standalone);
        // inspector is None by default.

        let (addr, cancel, _handle) = bind_server(state).await;
        let resp = reqwest::get(format!("http://{addr}/")).await.unwrap();
        assert_eq!(resp.status(), 200);
        let body = resp.text().await.unwrap();
        assert!(body.contains("/health"));
        assert!(body.contains("/status"));
        assert!(!body.contains("/api/topology"));

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_root_lists_api_endpoints_when_ui_disabled() {
        let mut state = make_state(ServiceModeLabel::Standalone);
        state.inspector = Some(crate::inspector::Inspector::new(
            &crate::inspector::InspectorConfig {
                ui: false,
                ..crate::inspector::InspectorConfig::default()
            },
        ));

        let (addr, cancel, _handle) = bind_server(state).await;
        let resp = reqwest::get(format!("http://{addr}/")).await.unwrap();
        assert_eq!(resp.status(), 200);
        let body = resp.text().await.unwrap();
        assert!(body.contains("/api/topology"));
        assert!(body.contains("/ui"));

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_health_returns_200_when_liveness_fresh() {
        let state = make_state(ServiceModeLabel::Standalone);
        // Immediately after start uptime is ~0 s and the liveness counter is 0,
        // so the minimum expected value saturates to 0 and the check passes.
        let (addr, cancel, _handle) = bind_server(state).await;
        let resp = reqwest::get(format!("http://{addr}/health")).await.unwrap();
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "alive");

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_health_returns_503_when_liveness_stale() {
        let mut state = make_state(ServiceModeLabel::Standalone);
        // Wind back started_at by 60 seconds so uptime ≈ 60 s.
        state.started_at = Instant::now() - Duration::from_secs(60);
        // Leave the liveness counter at 0, far below the minimum expected (~55).

        let (addr, cancel, _handle) = bind_server(state).await;
        let resp = reqwest::get(format!("http://{addr}/health")).await.unwrap();
        assert_eq!(resp.status(), 503);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "stale");

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_ready_returns_200_when_ready_true() {
        let state = make_state(ServiceModeLabel::Standalone);
        state.ready.store(true, Ordering::Relaxed);

        let (addr, cancel, _handle) = bind_server(state).await;
        let resp = reqwest::get(format!("http://{addr}/ready")).await.unwrap();
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "ready");

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_ready_returns_503_when_not_ready() {
        let state = make_state(ServiceModeLabel::Standalone);
        // ready is false by default.

        let (addr, cancel, _handle) = bind_server(state).await;
        let resp = reqwest::get(format!("http://{addr}/ready")).await.unwrap();
        assert_eq!(resp.status(), 503);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "not_ready");

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_metrics_returns_200_with_prometheus_body() {
        let state = make_state(ServiceModeLabel::Standalone);
        let (addr, cancel, _handle) = bind_server(state).await;
        let resp = reqwest::get(format!("http://{addr}/metrics"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        let content_type = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(
            content_type.contains("text/plain"),
            "expected text/plain content-type, got: {content_type}"
        );

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_status_standalone_mode_returns_correct_json() {
        let stats = Arc::new(RwLock::new(StandaloneStats {
            iterations: 42,
            rows_processed: 420_000,
            source_batches_drained: 120,
            sink_batches_written: 119,
            iteration_errors: 2,
            ..Default::default()
        }));

        let mut state = make_state(ServiceModeLabel::Standalone);
        state.standalone_stats = Some(vec![("w".to_string(), stats)]);

        let (addr, cancel, _handle) = bind_server(state).await;
        let resp = reqwest::get(format!("http://{addr}/status")).await.unwrap();
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["node_id"], 1);
        assert_eq!(body["mode"], "standalone");
        assert!(body["cluster"].is_null());
        assert!(!body["standalone"].is_null());
        assert_eq!(body["standalone"][0]["workflow_id"], "w");
        assert_eq!(body["standalone"][0]["iterations"], 42);
        assert_eq!(body["standalone"][0]["rows_processed"], 420_000);

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_status_cluster_mode_with_probe_returns_cluster_json() {
        struct MockProbe;
        impl ClusterProbe for MockProbe {
            fn snapshot(&self) -> ClusterProbeSnapshot {
                ClusterProbeSnapshot {
                    role: "leader",
                    cluster_health: "healthy",
                    current_leader: Some(1),
                    raft_term: 7,
                    last_log_index: 12345,
                    commit_index: 12345,
                    last_applied: 12345,
                    snapshot_last_applied: Some(12000),
                    membership: vec![(1, "10.0.0.1:9000".to_string())],
                    voters: vec![1, 2, 3],
                    learners: vec![],
                    batches_processed_total: 1234,
                    batches_failed_total: 5,
                }
            }
        }

        let mut state = make_state(ServiceModeLabel::Cluster);
        state.cluster_probe = Some(Arc::new(MockProbe));

        let (addr, cancel, _handle) = bind_server(state).await;
        let resp = reqwest::get(format!("http://{addr}/status")).await.unwrap();
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["mode"], "cluster");
        assert!(body["standalone"].is_null());
        assert!(!body["cluster"].is_null());
        assert_eq!(body["cluster"]["role"], "leader");
        assert_eq!(body["cluster"]["health"], "healthy");
        assert_eq!(body["cluster"]["raft_term"], 7);
        assert_eq!(body["cluster"]["batches_processed_total"], 1234);

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_status_cluster_mode_without_probe_returns_null_cluster() {
        let state = make_state(ServiceModeLabel::Cluster);
        // cluster_probe is None.

        let (addr, cancel, _handle) = bind_server(state).await;
        let resp = reqwest::get(format!("http://{addr}/status")).await.unwrap();
        assert_eq!(resp.status(), 200);

        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["mode"], "cluster");
        assert!(body["cluster"].is_null());

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_graceful_shutdown_exits_within_2_seconds() {
        let state = make_state(ServiceModeLabel::Standalone);
        let (addr, cancel, handle) = bind_server(state).await;

        let resp = reqwest::get(format!("http://{addr}/health")).await.unwrap();
        assert_eq!(resp.status(), 200);

        cancel.cancel();

        let res = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(res.is_ok(), "server shutdown took longer than 2 seconds");
        assert!(res.unwrap().unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_bind_failure_on_taken_port_returns_error() {
        // Bind a listener on port 0, then try to bind the same address.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let state = make_state(ServiceModeLabel::Standalone);
        let cfg = HttpConfig {
            bind: addr.to_string(),
            disabled: false,
            control: false,
        };
        let cancel = CancellationToken::new();

        let result = serve_http(&cfg, state, cancel).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.category(), "generic");
        assert!(
            err.message().contains("bind"),
            "error should mention bind: {err}"
        );

        drop(listener);
    }

    #[tokio::test]
    async fn test_http_disabled_returns_ok_immediately() {
        let state = make_state(ServiceModeLabel::Standalone);
        let cfg = HttpConfig {
            bind: "127.0.0.1:0".to_string(),
            disabled: true,
            control: false,
        };
        let cancel = CancellationToken::new();

        let result = serve_http(&cfg, state, cancel).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_watchdog_stops_on_cancellation() {
        let state = make_state(ServiceModeLabel::Standalone);
        let cancel = CancellationToken::new();
        let liveness = state.liveness.clone();

        let handle = spawn_watchdog(state, cancel.clone());

        // Wait a few ticks.
        tokio::time::sleep(Duration::from_millis(150)).await;
        let before = liveness.load(Ordering::Relaxed);
        assert!(before > 0, "watchdog should have ticked at least once");

        cancel.cancel();
        // Give the task time to exit.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let after = liveness.load(Ordering::Relaxed);
        // After a short wait the counter should have stabilised.
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let final_val = liveness.load(Ordering::Relaxed);

        // The task should have stopped; the counter must not grow significantly
        // more than ~1 (one in-flight tick may have started before cancel).
        assert!(
            final_val <= after + 1,
            "watchdog should stop after cancel: before={before}, after={after}, final={final_val}"
        );

        handle.abort();
    }
}
