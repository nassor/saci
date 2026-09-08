//! The dead letter queue API: read what is waiting, replay it, purge it.
//!
//! Merged into the control-plane router by
//! [`build_router`](super::http::build_router) only when at least one workflow
//! declares a `dlq` block, so a service with none has no endpoint here rather
//! than one answering an empty list, the same convention the inspector's and
//! the lifecycle plane's routes follow. The two `POST` routes need the
//! lifecycle registry as well, because a replay runs inside the workflow's
//! own runner and only a running workflow has one.
//!
//! | Route | Body |
//! |---|---|
//! | `GET /api/dlq` | [`DlqSummary`](saci_inspector_wire::DlqSummary) list, in declaration order |
//! | `GET /api/workflows/{id}/dlq` | one summary, 404 for a workflow with no queue |
//! | `POST /api/workflows/{id}/dlq/replay` | optional [`DlqReplayRequest`] body narrowing the replay |
//! | `POST /api/workflows/{id}/dlq/purge` | discard every letter instead of writing it |
//!
//! A `POST` hands the request to the runner and waits [`DLQ_ACK_BUDGET`] for
//! the report: `200` with a [`DlqReplayReport`](saci_inspector_wire::DlqReplayReport) once the replay ran, `202`
//! with the current summary while the runner has not reached its replay point
//! yet. Refusals are `404` (no queue), `409` (the workflow is not running, or
//! a replay is already pending) and `503` (the runner went away mid-request).
//!
//! The `202` is the normal answer for an idle workflow: both replay points sit
//! inside a pass, and in `run_mode kind="stream"` the head of the loop is
//! reached only when an item arrives. The request stays queued and the runner
//! serves it when it next gets there.

use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;

use saci_inspector_wire::{DlqReplayRequest, DlqTrigger, WorkflowRunState};

use super::dlq::{DlqError, ReplayFilter};
use super::http::ServiceState;

/// How long a `POST` waits for the runner to reach its replay point before
/// answering `202`.
///
/// Matches [`LIFECYCLE_ACK_BUDGET`](super::lifecycle::LIFECYCLE_ACK_BUDGET):
/// an operator action either lands within a few seconds or is better reported
/// as accepted than held open.
pub const DLQ_ACK_BUDGET: Duration = Duration::from_secs(5);

/// The read-only routes, available whenever a queue is declared.
pub fn read_routes() -> Router<ServiceState> {
    Router::new()
        .route("/api/dlq", get(handle_list))
        .route("/api/workflows/{id}/dlq", get(handle_get))
}

/// The two `POST` routes, available only alongside the lifecycle registry.
pub fn control_routes() -> Router<ServiceState> {
    Router::new()
        .route("/api/workflows/{id}/dlq/replay", post(handle_replay))
        .route("/api/workflows/{id}/dlq/purge", post(handle_purge))
}

/// The body of every refusal.
#[derive(Serialize)]
struct ControlError {
    error: String,
}

fn refuse(status: StatusCode, error: String) -> Response {
    (status, Json(ControlError { error })).into_response()
}

fn no_queue(id: &str) -> Response {
    refuse(
        StatusCode::NOT_FOUND,
        format!("workflow '{id}' has no dead letter queue"),
    )
}

/// `GET /api/dlq`
async fn handle_list(State(state): State<ServiceState>) -> Response {
    match &state.dlq {
        Some(registry) => Json(registry.list()).into_response(),
        None => unmounted(),
    }
}

/// `GET /api/workflows/{id}/dlq`
async fn handle_get(State(state): State<ServiceState>, Path(id): Path<String>) -> Response {
    let Some(registry) = &state.dlq else {
        return unmounted();
    };
    match registry.get(&id) {
        Some(shared) => Json(shared.summary()).into_response(),
        None => no_queue(&id),
    }
}

/// `POST /api/workflows/{id}/dlq/replay`
///
/// An absent or empty body replays every letter. `Option<Json<..>>` is what
/// makes a bodyless `curl -X POST` legal, which is the form the docs give.
async fn handle_replay(
    state: State<ServiceState>,
    id: Path<String>,
    body: Option<Json<DlqReplayRequest>>,
) -> Response {
    let request = body.map(|Json(request)| request).unwrap_or_default();
    request_replay(
        state,
        id,
        DlqTrigger::Manual,
        ReplayFilter {
            sink: request.sink,
            reason: request.reason,
        },
    )
    .await
}

/// `POST /api/workflows/{id}/dlq/purge`
async fn handle_purge(state: State<ServiceState>, id: Path<String>) -> Response {
    request_replay(state, id, DlqTrigger::Purge, ReplayFilter::default()).await
}

/// Both `POST` handlers' body.
async fn request_replay(
    State(state): State<ServiceState>,
    Path(id): Path<String>,
    trigger: DlqTrigger,
    filter: ReplayFilter,
) -> Response {
    let Some(registry) = &state.dlq else {
        return unmounted();
    };
    let Some(shared) = registry.get(&id) else {
        return no_queue(&id);
    };
    // A replay runs inside the runner, between passes, so a workflow that is
    // not running has nothing to run it.
    if let Some(lifecycle) = &state.lifecycle {
        let state_of = lifecycle.get(&id).map(|status| status.state);
        match state_of {
            Some(WorkflowRunState::Running) => {}
            Some(other) => {
                return refuse(
                    StatusCode::CONFLICT,
                    format!(
                        "workflow '{id}' is {}; only a running workflow replays",
                        other.as_str()
                    ),
                );
            }
            None => return no_queue(&id),
        }
    }
    let receiver = match shared.request(trigger, filter) {
        Ok(receiver) => receiver,
        Err(DlqError::Busy) => {
            return refuse(
                StatusCode::CONFLICT,
                format!("workflow '{id}' already has a replay pending"),
            );
        }
    };
    match tokio::time::timeout(DLQ_ACK_BUDGET, receiver).await {
        Ok(Ok(report)) => (StatusCode::OK, Json(report)).into_response(),
        // The runner dropped its end: the queue went with the runner.
        Ok(Err(_)) => refuse(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("workflow '{id}' is gone"),
        ),
        // Still queued. The summary carries `replay_pending`, so a poller
        // sees the request is live rather than lost.
        Err(_) => (StatusCode::ACCEPTED, Json(shared.summary())).into_response(),
    }
}

/// Unreachable in a normally-built router: these routes are merged only when
/// [`ServiceState::dlq`] is `Some`. A hand-built router that merges them
/// anyway gets an honest answer rather than a panic.
fn unmounted() -> Response {
    refuse(
        StatusCode::NOT_FOUND,
        "no workflow declares a dead letter queue on this service".to_string(),
    )
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64};

    use saci_inspector_wire::{DlqReplayReport, DlqSummary};

    use crate::service::config::{DlqBlock, DlqConfig, ServiceConfig};
    use crate::service::dlq::{DlqRegistry, DlqShared};
    use crate::service::http::{ServiceModeLabel, ServiceState, build_router};

    const FIXTURE: &str = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci-dlq-api"

workflow "orders" {
    source "in" type="NoopSource" component="X"
    sink "out" type="NoopSink" component="X"
    link from="in" to="out"
    dlq "redb"
}

workflow "plain" {
    source "in2" type="NoopSource" component="X"
    sink "out2" type="NoopSink" component="X"
    link from="in2" to="out2"
}
"#;

    fn config() -> ServiceConfig {
        serde::Deserialize::deserialize(
            crate::service::config::from_kdl_str(FIXTURE).expect("fixture parses"),
        )
        .expect("fixture deserializes")
    }

    fn state_with(dlq: Option<Arc<DlqRegistry>>) -> ServiceState {
        ServiceState {
            node_id: 1,
            node_name: None,
            mode: ServiceModeLabel::Standalone,
            started_at: std::time::Instant::now(),
            prometheus_registry: Arc::new(prometheus::Registry::new()),
            liveness: Arc::new(AtomicU64::new(0)),
            ready: Arc::new(AtomicBool::new(true)),
            cluster_probe: None,
            standalone_stats: None,
            inspector: None,
            lifecycle: None,
            dlq,
        }
    }

    /// Bind the router on an ephemeral port and answer its base URL.
    async fn serve(state: ServiceState) -> String {
        // The tests drive the router with a reqwest client, which refuses to
        // build without an installed crypto provider.
        crate::service::install_ring_provider();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr").to_string();
        tokio::spawn(async move {
            let _ = axum::serve(listener, build_router(state)).await;
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        format!("http://{addr}")
    }

    async fn get(base: &str, path: &str) -> (u16, String) {
        let response = reqwest::get(format!("{base}{path}"))
            .await
            .expect("request");
        let status = response.status().as_u16();
        (status, response.text().await.expect("body"))
    }

    async fn post(base: &str, path: &str) -> (u16, String) {
        let response = reqwest::Client::new()
            .post(format!("{base}{path}"))
            .send()
            .await
            .expect("request");
        let status = response.status().as_u16();
        (status, response.text().await.expect("body"))
    }

    /// A handler's answer as `(status, body)`, for the tests that call one
    /// directly rather than through a socket.
    async fn read(response: Response) -> (u16, String) {
        let status = response.status().as_u16();
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("body");
        (
            status,
            String::from_utf8(bytes.to_vec()).expect("utf8 body"),
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn with_no_queue_declared_the_routes_are_not_mounted_at_all() {
        let base = serve(state_with(None)).await;

        let (status, _) = get(&base, "/api/dlq").await;
        assert_eq!(status, 404, "the read routes are merged only with a queue");
        let (status, _) = post(&base, "/api/workflows/orders/dlq/replay").await;
        assert_eq!(status, 404);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_workflow_without_a_dlq_block_is_not_found() {
        let registry = Arc::new(DlqRegistry::from_config(&config()));
        let base = serve(state_with(Some(registry))).await;

        let (status, body) = get(&base, "/api/dlq").await;
        assert_eq!(status, 200);
        let summaries: Vec<DlqSummary> = serde_json::from_str(&body).expect("summaries");
        assert_eq!(summaries.len(), 1, "only 'orders' declares a queue");
        assert_eq!(summaries[0].workflow, "orders");
        assert_eq!(summaries[0].store, "redb");
        assert_eq!(summaries[0].replay, "before_sources");
        assert!(!summaries[0].known, "nothing has read the store yet");

        let (status, body) = get(&base, "/api/workflows/plain/dlq").await;
        assert_eq!(status, 404);
        assert!(
            body.contains("has no dead letter queue"),
            "message was {body}"
        );
    }

    #[tokio::test]
    async fn a_replay_answers_the_report_the_runner_sends_back() {
        let registry = Arc::new(DlqRegistry::from_config(&config()));
        let state = state_with(Some(Arc::clone(&registry)));
        let shared = Arc::clone(registry.get("orders").expect("declared queue"));

        // Stand in for the runner: take the queued request and answer it.
        let runner = tokio::spawn(async move {
            loop {
                if shared
                    .answer_for_test(DlqReplayReport {
                        trigger: DlqTrigger::Manual,
                        started_at_unix_ms: 1,
                        duration_ms: 2,
                        delivered: 3,
                        retained: 0,
                        purged: 0,
                        lost: 0,
                        error: None,
                    })
                    .is_some()
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        });

        let response = handle_replay(
            State(state),
            Path("orders".to_string()),
            Some(Json(DlqReplayRequest {
                sink: Some("out".to_string()),
                reason: None,
            })),
        )
        .await;
        runner.await.expect("runner task");

        let (status, body) = read(response).await;
        assert_eq!(status, 200, "body was {body}");
        let report: DlqReplayReport = serde_json::from_str(&body).expect("report");
        assert_eq!(report.delivered, 3);
        assert_eq!(report.trigger, DlqTrigger::Manual);
    }

    #[tokio::test(start_paused = true)]
    async fn a_replay_no_runner_answers_is_accepted_with_the_summary() {
        let registry = Arc::new(DlqRegistry::from_config(&config()));
        let state = state_with(Some(Arc::clone(&registry)));

        // No runner takes the request, so the budget runs out on the paused
        // clock and the handler answers with the summary instead.
        let response = handle_purge(State(state.clone()), Path("orders".to_string())).await;
        let (status, body) = read(response).await;
        assert_eq!(status, 202, "body was {body}");
        let summary: DlqSummary = serde_json::from_str(&body).expect("summary");
        assert!(
            summary.replay_pending,
            "the queued request is still live: {body}"
        );

        // The slot is occupied, so a second request is refused rather than
        // replacing the first.
        let response = handle_replay(State(state), Path("orders".to_string()), None).await;
        let (status, body) = read(response).await;
        assert_eq!(status, 409, "body was {body}");
        assert!(
            body.contains("already has a replay pending"),
            "message was {body}"
        );
    }

    #[test]
    fn the_registry_holds_one_entry_per_declared_block() {
        let registry = DlqRegistry::from_config(&config());
        assert!(!registry.is_empty());
        assert!(registry.get("orders").is_some());
        assert!(registry.get("plain").is_none());

        let empty = DlqRegistry::from_config(&ServiceConfig {
            workflows: Vec::new(),
            ..config()
        });
        assert!(empty.is_empty());
    }

    #[test]
    fn a_summary_reports_the_declared_store_and_replay_point() {
        let shared = DlqShared::new(
            "orders",
            &DlqBlock {
                store: "kafka".to_string(),
                ..DlqBlock::default()
            },
        );
        let summary = shared.summary();
        assert_eq!(summary.store, "kafka");
        assert_eq!(summary.replay, "before_sources");
        assert_eq!(summary.letters, 0);
        assert!(summary.groups.is_empty());
        assert!(summary.last_replay.is_none());

        // The scalar parse shape agrees with what the registry publishes.
        let parsed: DlqConfig =
            serde::Deserialize::deserialize(serde_json::json!("kafka")).expect("scalar form");
        assert_eq!(parsed.0.store, "kafka");
    }

    /// A replay runs inside the runner between passes, so a workflow that is
    /// not `Running` is refused rather than queued forever. A freshly
    /// registered, never-started workflow is `Starting`, which is enough to
    /// exercise the same "not running" branch a `Paused` or `Stopped`
    /// workflow would: `SupervisorChannels`'s status sender is private to
    /// `lifecycle.rs`, so a test outside it cannot publish an explicit
    /// `Paused`/`Stopped` status without driving a full `run_supervised`
    /// loop.
    #[tokio::test]
    async fn a_replay_is_refused_for_a_workflow_that_is_not_running() {
        let registry = Arc::new(DlqRegistry::from_config(&config()));
        let mut builder = crate::service::lifecycle::LifecycleRegistryBuilder::new();
        let _channels = builder.register("orders", None, None);
        let lifecycle = Arc::new(builder.build());
        let mut state = state_with(Some(registry));
        state.lifecycle = Some(lifecycle);

        let response = handle_replay(State(state), Path("orders".to_string()), None).await;
        let (status, body) = read(response).await;
        assert_eq!(status, 409, "body was {body}");
        assert!(
            body.contains("only a running workflow replays"),
            "message was {body}"
        );
    }
}
