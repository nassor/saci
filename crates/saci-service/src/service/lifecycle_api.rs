//! The workflow lifecycle API: read one or every workflow's state, and send a
//! verb to one workflow or to all of them at once.
//!
//! Merged into the control-plane router by
//! [`build_router`](super::http::build_router) **only** when the service is
//! running standalone and `http { control #true }` (the default), so a cluster
//! node or an operator who switched control off has no endpoint here rather
//! than a forbidden one, the same convention the inspector's routes follow.
//!
//! | Route | Body |
//! |---|---|
//! | `GET /api/workflows` | [`WorkflowStatus`](saci_inspector_wire::WorkflowStatus) list, in declaration order |
//! | `GET /api/workflows/{id}` | one `WorkflowStatus`, 404 for an unknown id |
//! | `POST /api/workflows/{id}/start` | build and run a stopped workflow |
//! | `POST /api/workflows/{id}/stop` | drain the runner and drop the built workflow |
//! | `POST /api/workflows/{id}/pause` | park the runner between passes |
//! | `POST /api/workflows/{id}/resume` | release a parked runner |
//! | `POST /api/workflows/{id}/restart` | stop, then start |
//! | `POST /api/service/{verb}` | the same five verbs, applied to every workflow at once |
//!
//! A per-workflow `POST` answers the resulting `WorkflowStatus`: `200` once the
//! transition settled, `202` when it was still in progress after
//! [`LIFECYCLE_ACK_BUDGET`](super::lifecycle::LIFECYCLE_ACK_BUDGET). A verb
//! that was already satisfied is `200` with the status unchanged. Refusals are
//! `404` (unknown workflow), `409` (illegal from this state, or the workflow
//! cannot be rebuilt) and `503` (the supervisor is gone).
//!
//! A `/api/service/` verb answers a
//! [`ServiceLifecycleReport`](saci_inspector_wire::ServiceLifecycleReport):
//! `200` when every workflow took it and settled, `202` when one is still in
//! progress, and `409` only when not one workflow accepted it. A workflow the
//! verb could not reach appears in `refused` rather than failing the request,
//! because a service-wide verb is what an operator reaches for *instead* of
//! checking each workflow's state first.
//!
//! `/api/service/` is a separate path rather than a reserved workflow id:
//! `pause` is a legal declared id, so `POST /api/workflows/pause` has to keep
//! meaning that workflow.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;

use super::http::ServiceState;
use super::lifecycle::{LifecycleError, WorkflowCommand};

/// The lifecycle routes, sharing the control plane's [`ServiceState`].
pub fn routes() -> Router<ServiceState> {
    Router::new()
        .route("/api/workflows", get(handle_list))
        .route("/api/workflows/{id}", get(handle_get))
        .route("/api/workflows/{id}/start", post(handle_start))
        .route("/api/workflows/{id}/stop", post(handle_stop))
        .route("/api/workflows/{id}/pause", post(handle_pause))
        .route("/api/workflows/{id}/resume", post(handle_resume))
        .route("/api/workflows/{id}/restart", post(handle_restart))
        .route("/api/service/start", post(handle_start_all))
        .route("/api/service/stop", post(handle_stop_all))
        .route("/api/service/pause", post(handle_pause_all))
        .route("/api/service/resume", post(handle_resume_all))
        .route("/api/service/restart", post(handle_restart_all))
}

/// The body of every refusal.
#[derive(Serialize)]
struct ControlError {
    error: String,
}

fn refuse(status: StatusCode, error: String) -> Response {
    (status, Json(ControlError { error })).into_response()
}

/// `GET /api/workflows`
async fn handle_list(State(state): State<ServiceState>) -> Response {
    match &state.lifecycle {
        Some(registry) => Json(registry.list()).into_response(),
        None => unmounted(),
    }
}

/// `GET /api/workflows/{id}`
async fn handle_get(State(state): State<ServiceState>, Path(id): Path<String>) -> Response {
    let Some(registry) = &state.lifecycle else {
        return unmounted();
    };
    match registry.get(&id) {
        Some(status) => Json(status).into_response(),
        None => refuse(
            StatusCode::NOT_FOUND,
            LifecycleError::UnknownWorkflow(id).to_string(),
        ),
    }
}

async fn handle_start(state: State<ServiceState>, id: Path<String>) -> Response {
    control(state, id, WorkflowCommand::Start).await
}

async fn handle_stop(state: State<ServiceState>, id: Path<String>) -> Response {
    control(state, id, WorkflowCommand::Stop).await
}

async fn handle_pause(state: State<ServiceState>, id: Path<String>) -> Response {
    control(state, id, WorkflowCommand::Pause).await
}

async fn handle_resume(state: State<ServiceState>, id: Path<String>) -> Response {
    control(state, id, WorkflowCommand::Resume).await
}

async fn handle_restart(state: State<ServiceState>, id: Path<String>) -> Response {
    control(state, id, WorkflowCommand::Restart).await
}

/// Every `POST` handler's body.
async fn control(
    State(state): State<ServiceState>,
    Path(id): Path<String>,
    command: WorkflowCommand,
) -> Response {
    let Some(registry) = &state.lifecycle else {
        return unmounted();
    };
    match registry.command(&id, command).await {
        Ok((status, true)) => (StatusCode::OK, Json(status)).into_response(),
        // Accepted, still in progress: the caller polls `GET /api/workflows`.
        Ok((status, false)) => (StatusCode::ACCEPTED, Json(status)).into_response(),
        Err(e @ LifecycleError::UnknownWorkflow(_)) => refuse(StatusCode::NOT_FOUND, e.to_string()),
        Err(
            e @ (LifecycleError::NotRestartable { .. } | LifecycleError::IllegalTransition { .. }),
        ) => refuse(StatusCode::CONFLICT, e.to_string()),
        Err(e @ LifecycleError::Gone(_)) => refuse(StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
    }
}

async fn handle_start_all(state: State<ServiceState>) -> Response {
    control_all(state, WorkflowCommand::Start).await
}

async fn handle_stop_all(state: State<ServiceState>) -> Response {
    control_all(state, WorkflowCommand::Stop).await
}

async fn handle_pause_all(state: State<ServiceState>) -> Response {
    control_all(state, WorkflowCommand::Pause).await
}

async fn handle_resume_all(state: State<ServiceState>) -> Response {
    control_all(state, WorkflowCommand::Resume).await
}

async fn handle_restart_all(state: State<ServiceState>) -> Response {
    control_all(state, WorkflowCommand::Restart).await
}

/// Every `/api/service/` handler's body.
async fn control_all(State(state): State<ServiceState>, command: WorkflowCommand) -> Response {
    let Some(registry) = &state.lifecycle else {
        return unmounted();
    };
    let report = registry.command_all(command).await;
    // A partial sweep is a success: the report names what the verb could not
    // reach. `409` is reserved for the case where it reached nothing, which is
    // the only one an operator has to act on.
    let status = if report.applied.is_empty() && !report.refused.is_empty() {
        StatusCode::CONFLICT
    } else if report.settled {
        StatusCode::OK
    } else {
        StatusCode::ACCEPTED
    };
    (status, Json(report)).into_response()
}

/// Unreachable in a normally-built router: these routes are merged only when
/// [`ServiceState::lifecycle`] is `Some`. A hand-built router that merges them
/// anyway gets an honest answer rather than a panic.
fn unmounted() -> Response {
    refuse(
        StatusCode::NOT_FOUND,
        "workflow lifecycle control is not enabled on this service".to_string(),
    )
}
