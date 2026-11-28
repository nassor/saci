//! HTTP read endpoints for OpenLineage dataset and column-level lineage.
//!
//! | Route | Body |
//! |---|---|
//! | `GET /api/lineage` | [`LineageSnapshot`](crate::service::lineage::LineageSnapshot) |
//! | `GET /api/workflows/{id}/lineage` | [`WorkflowLineageGraph`](crate::service::lineage::WorkflowLineageGraph), or 404 |

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};

use super::{ServiceState, refuse};

/// Routes mounted when lineage state is attached to [`ServiceState`].
pub(super) fn routes() -> Router<ServiceState> {
    Router::new()
        .route("/api/lineage", get(handle_service_lineage))
        .route("/api/workflows/{id}/lineage", get(handle_workflow_lineage))
}

async fn handle_service_lineage(State(state): State<ServiceState>) -> Response {
    let Some(dispatcher) = &state.lineage else {
        return refuse(
            StatusCode::NOT_FOUND,
            "lineage is not configured on this service".to_string(),
        );
    };
    (StatusCode::OK, Json(dispatcher.snapshot())).into_response()
}

async fn handle_workflow_lineage(
    State(state): State<ServiceState>,
    Path(id): Path<String>,
) -> Response {
    let Some(dispatcher) = &state.lineage else {
        return refuse(
            StatusCode::NOT_FOUND,
            "lineage is not configured on this service".to_string(),
        );
    };
    match dispatcher.workflow_lineage(&id) {
        Some(graph) => (StatusCode::OK, Json(graph)).into_response(),
        None => refuse(
            StatusCode::NOT_FOUND,
            format!("no workflow lineage found for '{id}'"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64},
    };
    use std::time::Instant;

    use tokio::net::TcpListener;
    use tokio_util::sync::CancellationToken;

    use crate::service::config::ServiceConfig;
    use crate::service::config::test_support::parse;
    use crate::service::http::{ServiceModeLabel, ServiceState, build_router};
    use crate::service::lineage::LineageDispatcher;

    fn test_config() -> ServiceConfig {
        parse(
            r#"
mode "standalone"
node id=1 data_dir="/tmp/saci-lineage-api"
lineage enabled=#true namespace="catalog"

workflow "wf1" {
    source "s" connector="file" component="Item" {
        config path="/in.csv" {
            field "id" type="int64" nullable=#false
        }
    }
    sink "k" connector="file" component="Item" {
        config path="/out.parquet" {
            field "id" type="int64" nullable=#false
        }
    }
    link from="s" to="k"
}
"#,
        )
        .expect("config parses")
    }

    #[tokio::test]
    async fn lineage_endpoints_serve_graphs_and_404_on_unknown_workflow() {
        crate::service::install_ring_provider();
        let cfg = test_config();
        let dispatcher = Arc::new(LineageDispatcher::new(&cfg, None));
        let state = ServiceState {
            node_id: 1,
            node_name: Some("node-1".to_string()),
            mode: ServiceModeLabel::Standalone,
            started_at: Instant::now(),
            prometheus_registry: Arc::new(prometheus::Registry::new()),
            liveness: Arc::new(AtomicU64::new(0)),
            ready: Arc::new(AtomicBool::new(true)),
            standalone_stats: None,
            inspector: None,
            lifecycle: None,
            dlq: None,
            lineage: Some(dispatcher),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let cancel = CancellationToken::new();
        let cancel_srv = cancel.clone();
        tokio::spawn(async move {
            let _ = axum::serve(listener, build_router(state))
                .with_graceful_shutdown(cancel_srv.cancelled_owned())
                .await;
        });

        let client = reqwest::Client::new();
        let all_resp = client
            .get(format!("http://{addr}/api/lineage"))
            .send()
            .await
            .unwrap();
        assert_eq!(all_resp.status(), 200);
        let body: serde_json::Value = all_resp.json().await.unwrap();
        assert_eq!(body["namespace"], "catalog");
        assert_eq!(body["workflows"][0]["workflowId"], "wf1");

        let one_resp = client
            .get(format!("http://{addr}/api/workflows/wf1/lineage"))
            .send()
            .await
            .unwrap();
        assert_eq!(one_resp.status(), 200);
        let missing = client
            .get(format!("http://{addr}/api/workflows/unknown/lineage"))
            .send()
            .await
            .unwrap();
        assert_eq!(missing.status(), 404);

        cancel.cancel();
    }
}
