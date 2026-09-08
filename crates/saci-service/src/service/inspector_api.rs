//! The inspector's read-only JSON API, and the dashboard assets.
//!
//! Merged into the control-plane router by
//! [`build_router`](super::http::build_router) **only** when
//! `observability.inspector.enabled` is true, so a disabled inspector answers
//! 404 rather than 403: an operator who switched capture off has no endpoint
//! here, not a forbidden one.
//!
//! | Route | Body |
//! |---|---|
//! | `GET /api/topology` | [`Topology`](saci_inspector_wire::Topology) |
//! | `GET /api/snapshot?window_secs=60` | [`Snapshot`](saci_inspector_wire::Snapshot), the one document the dashboard polls |
//! | `GET /api/traces?limit=100` | newest-first [`TraceSummary`](saci_inspector_wire::TraceSummary) list |
//! | `GET /api/traces/{trace_id}` | [`TraceDetail`](saci_inspector_wire::TraceDetail) for the waterfall, 404 once the trace ages out |
//! | `GET /api/logs?limit=200&level=warn` | newest-first [`LogRecord`](saci_inspector_wire::LogRecord) list, filtered at or above `level` |
//! | `GET /ui`, `/ui/app.js`, `/ui/app_bg.wasm`, `/ui/app.css` | the dashboard bundle |
//!
//! Every endpoint reads the ring buffers and returns; none of them mutate
//! anything, and none of them block a pipeline.

use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;

use super::http::ServiceState;
use crate::inspector::Inspector;

/// Largest history window a client may ask for, in seconds.
///
/// Bounded because the caller controls it and a huge value would make the
/// handler walk the whole buffer for points it cannot use; the retention
/// window is the real ceiling anyway.
const MAX_WINDOW_SECS: u64 = 24 * 3600;

/// Largest number of records a list endpoint returns.
const MAX_LIMIT: usize = 1000;

fn default_window_secs() -> u64 {
    60
}

fn default_trace_limit() -> usize {
    100
}

fn default_log_limit() -> usize {
    200
}

/// `?window_secs=` for `/api/snapshot`.
#[derive(Debug, Deserialize)]
pub struct SnapshotQuery {
    #[serde(default = "default_window_secs")]
    window_secs: u64,
}

/// `?limit=` for `/api/traces`.
#[derive(Debug, Deserialize)]
pub struct TracesQuery {
    #[serde(default = "default_trace_limit")]
    limit: usize,
}

/// `?limit=`, `?level=` for `/api/logs`.
#[derive(Debug, Deserialize)]
pub struct LogsQuery {
    #[serde(default = "default_log_limit")]
    limit: usize,
    #[serde(default)]
    level: Option<String>,
}

/// The inspector routes, sharing the control plane's [`ServiceState`].
///
/// One state type across the whole router keeps `build_router` a single chain;
/// the handlers read [`ServiceState::inspector`].
pub fn routes() -> Router<ServiceState> {
    Router::new()
        .route("/api/topology", get(handle_topology))
        .route("/api/snapshot", get(handle_snapshot))
        .route("/api/traces", get(handle_traces))
        .route("/api/traces/{trace_id}", get(handle_trace))
        .route("/api/logs", get(handle_logs))
}

/// The dashboard asset routes, separate because `inspector.ui = false` drops
/// these while keeping the JSON API.
pub fn ui_routes() -> Router<ServiceState> {
    Router::new()
        .route("/ui", get(handle_ui_index))
        .route("/ui/", get(handle_ui_index))
        .route("/ui/app.js", get(handle_ui_js))
        .route("/ui/app_bg.wasm", get(handle_ui_wasm))
        .route("/ui/app.css", get(handle_ui_css))
}

/// The dashboard bundle, built by `cargo xtask ui` and committed under this
/// crate's own `assets/ui/` rather than under `saci-service-ui`: `cargo
/// package`/`publish` never includes files outside this crate's own
/// directory, so `include_str!`/`include_bytes!` cannot reach
/// `saci-service-ui` directly (it is also excluded from the workspace, being
/// wasm32-unknown-unknown only) without breaking a published tarball.
/// Embedded rather than read from disk so `saci-service` is one
/// self-contained binary and the dashboard cannot go missing at runtime.
const UI_INDEX_HTML: &str = include_str!("../../assets/ui/index.html");
const UI_APP_JS: &str = include_str!("../../assets/ui/app.js");
const UI_APP_WASM: &[u8] = include_bytes!("../../assets/ui/app_bg.wasm");
const UI_APP_CSS: &str = include_str!("../../assets/ui/app.css");

/// Every asset is served `no-cache`: the bundle changes with the binary, and a
/// stale cached `app.js` against a new `app_bg.wasm` is a hard `wasm-bindgen`
/// schema panic rather than a cosmetic mismatch.
const NO_CACHE: (header::HeaderName, &str) = (header::CACHE_CONTROL, "no-cache");

/// Pull the inspector out of the request state.
///
/// Unreachable in a normally-built router, because these routes are merged
/// only when the inspector exists. The `None` arm therefore answers the same
/// 404 a disabled inspector produces rather than a 500 that would suggest a
/// fault.
fn inspector_of(state: &ServiceState) -> Result<&Inspector, StatusCode> {
    state.inspector.as_ref().ok_or(StatusCode::NOT_FOUND)
}

async fn handle_topology(State(state): State<ServiceState>) -> impl IntoResponse {
    match inspector_of(&state) {
        Ok(inspector) => Json(inspector.topology()).into_response(),
        Err(status) => status.into_response(),
    }
}

async fn handle_snapshot(
    State(state): State<ServiceState>,
    Query(query): Query<SnapshotQuery>,
) -> impl IntoResponse {
    let inspector = match inspector_of(&state) {
        Ok(inspector) => inspector,
        Err(status) => return status.into_response(),
    };
    let window = Duration::from_secs(query.window_secs.clamp(1, MAX_WINDOW_SECS));
    let ready = state.ready.load(Ordering::Relaxed);
    Json(inspector.snapshot(window, ready)).into_response()
}

async fn handle_traces(
    State(state): State<ServiceState>,
    Query(query): Query<TracesQuery>,
) -> impl IntoResponse {
    match inspector_of(&state) {
        Ok(inspector) => Json(inspector.traces(query.limit.clamp(1, MAX_LIMIT))).into_response(),
        Err(status) => status.into_response(),
    }
}

async fn handle_trace(
    State(state): State<ServiceState>,
    Path(trace_id): Path<u64>,
) -> impl IntoResponse {
    let inspector = match inspector_of(&state) {
        Ok(inspector) => inspector,
        Err(status) => return status.into_response(),
    };
    match inspector.trace(trace_id) {
        Some(detail) => Json(detail).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn handle_logs(
    State(state): State<ServiceState>,
    Query(query): Query<LogsQuery>,
) -> impl IntoResponse {
    let inspector = match inspector_of(&state) {
        Ok(inspector) => inspector,
        Err(status) => return status.into_response(),
    };
    Json(inspector.logs(query.limit.clamp(1, MAX_LIMIT), query.level.as_deref())).into_response()
}

async fn handle_ui_index() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8"), NO_CACHE],
        UI_INDEX_HTML,
    )
}

async fn handle_ui_js() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/javascript; charset=utf-8"),
            NO_CACHE,
        ],
        UI_APP_JS,
    )
}

/// `application/wasm` is required, not cosmetic: the page loads the module with
/// `WebAssembly.instantiateStreaming`, which rejects any other content type.
async fn handle_ui_wasm() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "application/wasm"), NO_CACHE],
        UI_APP_WASM,
    )
}

async fn handle_ui_css() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8"), NO_CACHE],
        UI_APP_CSS,
    )
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;

    #[test]
    fn embedded_assets_are_present_and_non_empty() {
        assert!(
            UI_INDEX_HTML.contains("saci-app"),
            "index.html must carry the mount point the WASM entry point looks up"
        );
        assert!(!UI_APP_JS.is_empty(), "app.js missing: run cargo xtask ui");
        assert!(
            UI_APP_WASM.starts_with(b"\0asm"),
            "app_bg.wasm is not a WebAssembly module"
        );
        assert!(
            !UI_APP_CSS.is_empty(),
            "app.css missing: run cargo xtask ui"
        );
    }
}
