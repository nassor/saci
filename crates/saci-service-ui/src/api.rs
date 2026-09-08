//! Typed fetches against the inspector's JSON API.
//!
//! One function per endpoint, each returning the shared wire type. Errors are
//! `String` because the only thing the UI does with one is show it: a typed
//! error enum would carry no information a viewer can act on differently.

use saci_inspector_wire::{
    DlqReplayReport, DlqReplayRequest, DlqSummary, LogRecord, ServiceLifecycleReport, Snapshot,
    Topology, TraceDetail, TraceSummary, WorkflowStatus,
};
use serde::de::DeserializeOwned;

/// GET `url` and decode the body.
async fn get_json<T: DeserializeOwned>(url: &str) -> Result<T, String> {
    let response = gloo_net::http::Request::get(url)
        .send()
        .await
        .map_err(|e| format!("{url}: {e}"))?;
    if !response.ok() {
        return Err(format!("{url}: HTTP {}", response.status()));
    }
    response
        .json::<T>()
        .await
        .map_err(|e| format!("{url}: {e}"))
}

/// The static shape of the running workflow.
pub async fn topology() -> Result<Topology, String> {
    get_json("/api/topology").await
}

/// One frame of live numbers.
pub async fn snapshot(window_secs: u64) -> Result<Snapshot, String> {
    get_json(&format!("/api/snapshot?window_secs={window_secs}")).await
}

/// Newest-first trace list.
pub async fn traces(limit: usize) -> Result<Vec<TraceSummary>, String> {
    get_json(&format!("/api/traces?limit={limit}")).await
}

/// Spans and logs of one trace.
pub async fn trace(trace_id: u64) -> Result<TraceDetail, String> {
    get_json(&format!("/api/traces/{trace_id}")).await
}

/// Newest-first log tail, optionally filtered at or above `level`.
pub async fn logs(limit: usize, level: Option<&str>) -> Result<Vec<LogRecord>, String> {
    match level {
        Some(level) => get_json(&format!("/api/logs?limit={limit}&level={level}")).await,
        None => get_json(&format!("/api/logs?limit={limit}")).await,
    }
}

/// Lifecycle state of every controllable workflow.
///
/// `Ok(None)` means the control plane is not mounted: a cluster node, or
/// `http { control #false }`. That is not an error the viewer can act on, so
/// the dashboard hides the controls rather than showing a message.
pub async fn workflows() -> Result<Option<Vec<WorkflowStatus>>, String> {
    let url = "/api/workflows";
    let response = gloo_net::http::Request::get(url)
        .send()
        .await
        .map_err(|e| format!("{url}: {e}"))?;
    if response.status() == 404 {
        return Ok(None);
    }
    if !response.ok() {
        return Err(format!("{url}: HTTP {}", response.status()));
    }
    response
        .json::<Vec<WorkflowStatus>>()
        .await
        .map(Some)
        .map_err(|e| format!("{url}: {e}"))
}

/// Send one lifecycle verb to one workflow.
///
/// `200` is a settled transition and `202` one still in progress; both carry
/// the resulting [`WorkflowStatus`]. Any other status is the service's own
/// `{"error": ...}` message, which is what the header chip shows.
pub async fn control(id: &str, verb: &str) -> Result<WorkflowStatus, String> {
    let url = format!("/api/workflows/{id}/{verb}");
    let response = gloo_net::http::Request::post(&url)
        .send()
        .await
        .map_err(|e| format!("{url}: {e}"))?;
    let status = response.status();
    if status != 200 && status != 202 {
        let detail = response
            .json::<ControlError>()
            .await
            .map(|body| body.error)
            .unwrap_or_else(|_| format!("HTTP {status}"));
        return Err(format!("{url}: {detail}"));
    }
    response
        .json::<WorkflowStatus>()
        .await
        .map_err(|e| format!("{url}: {e}"))
}

/// Send one lifecycle verb to every controllable workflow.
///
/// `200` and `202` both carry the report; `409` means not one workflow
/// accepted the verb, and its own report names why, which is a better message
/// than the status code.
pub async fn control_all(verb: &str) -> Result<ServiceLifecycleReport, String> {
    let url = format!("/api/service/{verb}");
    let response = gloo_net::http::Request::post(&url)
        .send()
        .await
        .map_err(|e| format!("{url}: {e}"))?;
    let status = response.status();
    let report = response
        .json::<ServiceLifecycleReport>()
        .await
        .map_err(|e| format!("{url}: {e}"))?;
    if status != 200 && status != 202 {
        let detail = report
            .refused
            .first()
            .map_or_else(|| format!("HTTP {status}"), |first| first.error.clone());
        return Err(format!("{url}: {detail}"));
    }
    Ok(report)
}

/// Every declared dead letter queue.
///
/// `Ok(None)` means no workflow declares a `dlq` block, which is what
/// `/api/dlq` answering 404 reports, the same convention
/// [`workflows`] follows. The tab shows its empty state rather than an error.
pub async fn dlq() -> Result<Option<Vec<DlqSummary>>, String> {
    let url = "/api/dlq";
    let response = gloo_net::http::Request::get(url)
        .send()
        .await
        .map_err(|e| format!("{url}: {e}"))?;
    if response.status() == 404 {
        return Ok(None);
    }
    if !response.ok() {
        return Err(format!("{url}: HTTP {}", response.status()));
    }
    response
        .json::<Vec<DlqSummary>>()
        .await
        .map(Some)
        .map_err(|e| format!("{url}: {e}"))
}

/// Ask one workflow to replay its letters, optionally only those one sink
/// refused or those carrying one reason.
pub async fn dlq_replay(
    id: &str,
    sink: Option<&str>,
    reason: Option<&str>,
) -> Result<Option<DlqReplayReport>, String> {
    let body = DlqReplayRequest {
        sink: sink.map(str::to_string),
        reason: reason.map(str::to_string),
    };
    let url = format!("/api/workflows/{id}/dlq/replay");
    let request = gloo_net::http::Request::post(&url)
        .json(&body)
        .map_err(|e| format!("{url}: {e}"))?;
    post_replay(&url, request).await
}

/// Ask one workflow to discard every letter it holds.
pub async fn dlq_purge(id: &str) -> Result<Option<DlqReplayReport>, String> {
    let url = format!("/api/workflows/{id}/dlq/purge");
    let request = gloo_net::http::Request::post(&url)
        .build()
        .map_err(|e| format!("{url}: {e}"))?;
    post_replay(&url, request).await
}

/// Both replay verbs' answer.
///
/// `Ok(Some(report))` is a replay that ran, `Ok(None)` a `202`: the request
/// is queued and the runner serves it at its next replay point. That is a
/// notice rather than an error, so it is not an `Err`, which the header would
/// show in the destructive colour.
async fn post_replay(
    url: &str,
    request: gloo_net::http::Request,
) -> Result<Option<DlqReplayReport>, String> {
    let response = request.send().await.map_err(|e| format!("{url}: {e}"))?;
    let status = response.status();
    if status == 202 {
        return Ok(None);
    }
    if status != 200 {
        let detail = response
            .json::<ControlError>()
            .await
            .map(|body| body.error)
            .unwrap_or_else(|_| format!("HTTP {status}"));
        return Err(format!("{url}: {detail}"));
    }
    response
        .json::<DlqReplayReport>()
        .await
        .map(Some)
        .map_err(|e| format!("{url}: {e}"))
}

/// The service's refusal body.
#[derive(serde::Deserialize)]
struct ControlError {
    error: String,
}
