//! Outbound alerting and webhook notifications for service events.
//!
//! Configured via the top-level `alerts { ... }` block in `ServiceConfig`.
//! Supports generic HTTP webhooks, Slack Incoming Webhooks, and PagerDuty
//! Events API v2, with per-target event filtering and cooldown deduplication.
//!
//! ```kdl
//! alerts cooldown_ms=60000 timeout_ms=5000 {
//!     webhook "ops-hook" url="https://hooks.example.com/saci" {
//!         header Authorization="Bearer ${ALERT_TOKEN}"
//!         events "heal_exhausted" "workflow_failed" "dlq_recorded" "dlq_lost"
//!     }
//!     slack "ops-slack" webhook_url="https://hooks.slack.com/services/T/B/X" channel="#alerts"
//!     pagerduty "ops-pd" routing_key="${PD_ROUTING_KEY}" severity="critical"
//! }
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::config::one_or_many;
use crate::error::{SaciError, SaciResult};

use super::config::flexible::{de_opt_u64_flexible, de_u64_flexible};

/// Default HTTP delivery timeout in milliseconds.
const DEFAULT_ALERT_TIMEOUT_MS: u64 = 5_000;

/// Default PagerDuty Events API v2 enqueue endpoint.
const DEFAULT_PAGERDUTY_URL: &str = "https://events.pagerduty.com/v2/enqueue";

/// Event kinds that can trigger an outbound alert.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertEventKind {
    /// A connector exhausted its configured `heal.max_attempts` budget.
    HealExhausted,
    /// A supervised workflow transitioned to `WorkflowRunState::Failed`.
    WorkflowFailed,
    /// A refused batch was recorded in the dead letter queue.
    DlqRecorded,
    /// One or more dead letters were lost because the DLQ store failed or could not encode.
    DlqLost,
}

impl AlertEventKind {
    /// Canonical `snake_case` name used in KDL `events` lists and webhook JSON payloads.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::HealExhausted => "heal_exhausted",
            Self::WorkflowFailed => "workflow_failed",
            Self::DlqRecorded => "dlq_recorded",
            Self::DlqLost => "dlq_lost",
        }
    }

    /// Severity string for webhook and PagerDuty payloads.
    pub fn default_severity(self) -> &'static str {
        match self {
            Self::HealExhausted | Self::WorkflowFailed | Self::DlqLost => "critical",
            Self::DlqRecorded => "warning",
        }
    }
}

/// Deserialize `events` in either `"a"` or `["a", "b"]` form, or `None` when omitted.
fn de_opt_events<'de, D>(deserializer: D) -> Result<Option<Vec<AlertEventKind>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrManyEvents {
        One(AlertEventKind),
        Many(Vec<AlertEventKind>),
    }
    match Option::<OneOrManyEvents>::deserialize(deserializer)? {
        None => Ok(None),
        Some(OneOrManyEvents::One(e)) => Ok(Some(vec![e])),
        Some(OneOrManyEvents::Many(list)) => Ok(Some(list)),
    }
}

fn default_timeout_ms() -> u64 {
    DEFAULT_ALERT_TIMEOUT_MS
}

fn is_default_timeout(value: &u64) -> bool {
    *value == DEFAULT_ALERT_TIMEOUT_MS
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

fn default_pagerduty_url() -> String {
    DEFAULT_PAGERDUTY_URL.to_string()
}

fn is_default_pagerduty_url(value: &str) -> bool {
    value == DEFAULT_PAGERDUTY_URL
}

fn default_pagerduty_severity() -> String {
    "error".to_string()
}

fn is_default_pagerduty_severity(value: &str) -> bool {
    value == "error"
}

/// Generic HTTP JSON webhook target (`webhook "<id>" url="..."`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebhookTargetSpec {
    /// Target identifier (from the node's leading argument).
    pub id: String,
    /// Destination HTTP/HTTPS URL that receives `POST` JSON alerts.
    pub url: String,
    /// Optional HTTP headers sent with each alert request.
    #[serde(
        rename = "header",
        default,
        skip_serializing_if = "HashMap::is_empty",
        serialize_with = "super::config::serialize_sorted"
    )]
    pub headers: HashMap<String, String>,
    /// Optional filter of event kinds; when `None`, all event kinds are delivered.
    #[serde(
        default,
        deserialize_with = "de_opt_events",
        skip_serializing_if = "Option::is_none"
    )]
    pub events: Option<Vec<AlertEventKind>>,
    /// Optional per-target cooldown override in milliseconds.
    #[serde(
        default,
        deserialize_with = "de_opt_u64_flexible",
        skip_serializing_if = "Option::is_none"
    )]
    pub cooldown_ms: Option<u64>,
}

/// Slack Incoming Webhook target (`slack "<id>" webhook_url="..."`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlackTargetSpec {
    /// Target identifier.
    pub id: String,
    /// Slack Incoming Webhook URL.
    pub webhook_url: String,
    /// Optional channel override (e.g. `"#ops-alerts"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    /// Optional display username override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// Optional filter of event kinds; when `None`, all event kinds are delivered.
    #[serde(
        default,
        deserialize_with = "de_opt_events",
        skip_serializing_if = "Option::is_none"
    )]
    pub events: Option<Vec<AlertEventKind>>,
    /// Optional per-target cooldown override in milliseconds.
    #[serde(
        default,
        deserialize_with = "de_opt_u64_flexible",
        skip_serializing_if = "Option::is_none"
    )]
    pub cooldown_ms: Option<u64>,
}

/// PagerDuty Events API v2 target (`pagerduty "<id>" routing_key="..."`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PagerDutyTargetSpec {
    /// Target identifier.
    pub id: String,
    /// PagerDuty integration routing key.
    pub routing_key: String,
    /// Events API v2 URL (defaults to `https://events.pagerduty.com/v2/enqueue`).
    #[serde(
        default = "default_pagerduty_url",
        skip_serializing_if = "is_default_pagerduty_url"
    )]
    pub url: String,
    /// Event severity (`"critical"`, `"error"`, `"warning"`, or `"info"`).
    #[serde(
        default = "default_pagerduty_severity",
        skip_serializing_if = "is_default_pagerduty_severity"
    )]
    pub severity: String,
    /// Optional filter of event kinds; when `None`, all event kinds are delivered.
    #[serde(
        default,
        deserialize_with = "de_opt_events",
        skip_serializing_if = "Option::is_none"
    )]
    pub events: Option<Vec<AlertEventKind>>,
    /// Optional per-target cooldown override in milliseconds.
    #[serde(
        default,
        deserialize_with = "de_opt_u64_flexible",
        skip_serializing_if = "Option::is_none"
    )]
    pub cooldown_ms: Option<u64>,
}

/// Top-level `alerts { ... }` configuration block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AlertsConfig {
    /// Default deduplication cooldown window in milliseconds per `(target, event, workflow, node)`.
    /// `0` disables cooldown deduplication.
    #[serde(
        deserialize_with = "de_u64_flexible",
        skip_serializing_if = "is_zero_u64"
    )]
    pub cooldown_ms: u64,
    /// HTTP request timeout per outbound alert delivery in milliseconds.
    #[serde(
        default = "default_timeout_ms",
        deserialize_with = "de_u64_flexible",
        skip_serializing_if = "is_default_timeout"
    )]
    pub timeout_ms: u64,
    /// Generic HTTP webhook targets.
    #[serde(
        rename = "webhook",
        default,
        deserialize_with = "one_or_many",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub webhooks: Vec<WebhookTargetSpec>,
    /// Slack Incoming Webhook targets.
    #[serde(
        default,
        deserialize_with = "one_or_many",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub slack: Vec<SlackTargetSpec>,
    /// PagerDuty Events API v2 targets.
    #[serde(
        default,
        deserialize_with = "one_or_many",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub pagerduty: Vec<PagerDutyTargetSpec>,
}

impl Default for AlertsConfig {
    fn default() -> Self {
        Self {
            cooldown_ms: 0,
            timeout_ms: DEFAULT_ALERT_TIMEOUT_MS,
            webhooks: Vec::new(),
            slack: Vec::new(),
            pagerduty: Vec::new(),
        }
    }
}

impl AlertsConfig {
    /// Whether any alert target is declared.
    pub fn is_empty(&self) -> bool {
        self.webhooks.is_empty() && self.slack.is_empty() && self.pagerduty.is_empty()
    }

    /// Validate alert target declarations.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] for duplicate target IDs, empty URLs/routing keys,
    /// invalid PagerDuty severity, or zero timeout.
    pub fn validate(&self) -> SaciResult<()> {
        if self.timeout_ms == 0 {
            return Err(SaciError::configuration(
                "alerts.timeout_ms must be at least 1",
            ));
        }
        let mut seen = std::collections::HashSet::new();
        for w in &self.webhooks {
            if w.id.trim().is_empty() {
                return Err(SaciError::configuration(
                    "alerts.webhook: id must not be empty",
                ));
            }
            if !seen.insert(w.id.as_str()) {
                return Err(SaciError::configuration(format!(
                    "alerts: duplicate target id '{}'",
                    w.id
                )));
            }
            if w.url.trim().is_empty() {
                return Err(SaciError::configuration(format!(
                    "alerts.webhook '{}': url must not be empty",
                    w.id
                )));
            }
        }
        for s in &self.slack {
            if s.id.trim().is_empty() {
                return Err(SaciError::configuration(
                    "alerts.slack: id must not be empty",
                ));
            }
            if !seen.insert(s.id.as_str()) {
                return Err(SaciError::configuration(format!(
                    "alerts: duplicate target id '{}'",
                    s.id
                )));
            }
            if s.webhook_url.trim().is_empty() {
                return Err(SaciError::configuration(format!(
                    "alerts.slack '{}': webhook_url must not be empty",
                    s.id
                )));
            }
        }
        for p in &self.pagerduty {
            if p.id.trim().is_empty() {
                return Err(SaciError::configuration(
                    "alerts.pagerduty: id must not be empty",
                ));
            }
            if !seen.insert(p.id.as_str()) {
                return Err(SaciError::configuration(format!(
                    "alerts: duplicate target id '{}'",
                    p.id
                )));
            }
            if p.routing_key.trim().is_empty() {
                return Err(SaciError::configuration(format!(
                    "alerts.pagerduty '{}': routing_key must not be empty",
                    p.id
                )));
            }
            if p.url.trim().is_empty() {
                return Err(SaciError::configuration(format!(
                    "alerts.pagerduty '{}': url must not be empty",
                    p.id
                )));
            }
            if !matches!(
                p.severity.as_str(),
                "critical" | "error" | "warning" | "info"
            ) {
                return Err(SaciError::configuration(format!(
                    "alerts.pagerduty '{}': severity must be one of critical, error, warning, info (got '{}')",
                    p.id, p.severity
                )));
            }
        }
        Ok(())
    }
}

/// One alert event emitted by the service runtime.
#[derive(Debug, Clone, Serialize)]
pub struct AlertEvent {
    /// What happened.
    pub kind: AlertEventKind,
    /// Workflow ID that experienced the event.
    pub workflow: String,
    /// Optional node ID (`source` or `sink`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// Human-readable alert message.
    pub message: String,
    /// Unix timestamp in milliseconds when the event was created.
    pub at_unix_ms: u64,
    /// Structured event metadata.
    pub details: serde_json::Value,
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl AlertEvent {
    /// Construct a `heal_exhausted` event.
    pub fn heal_exhausted(
        workflow: &str,
        node: &str,
        role: &str,
        max_attempts: u32,
        last_error: Option<&str>,
    ) -> Self {
        Self {
            kind: AlertEventKind::HealExhausted,
            workflow: workflow.to_string(),
            node: Some(node.to_string()),
            message: format!(
                "workflow '{workflow}' {role} '{node}' exhausted {max_attempts} heal attempt(s){}",
                last_error.map(|e| format!(": {e}")).unwrap_or_default()
            ),
            at_unix_ms: now_unix_ms(),
            details: serde_json::json!({
                "role": role,
                "max_attempts": max_attempts,
                "last_error": last_error,
            }),
        }
    }

    /// Construct a `workflow_failed` event.
    pub fn workflow_failed(workflow: &str, error: &str, runs: u64) -> Self {
        Self {
            kind: AlertEventKind::WorkflowFailed,
            workflow: workflow.to_string(),
            node: None,
            message: format!("workflow '{workflow}' failed on run #{runs}: {error}"),
            at_unix_ms: now_unix_ms(),
            details: serde_json::json!({
                "runs": runs,
                "error": error,
            }),
        }
    }

    /// Construct a `dlq_recorded` event.
    pub fn dlq_recorded(
        workflow: &str,
        sink: &str,
        component: &str,
        rows: u64,
        reason: &str,
        pending_letters: u64,
    ) -> Self {
        Self {
            kind: AlertEventKind::DlqRecorded,
            workflow: workflow.to_string(),
            node: Some(sink.to_string()),
            message: format!(
                "workflow '{workflow}' sink '{sink}' dead-lettered {rows} row(s): {reason}"
            ),
            at_unix_ms: now_unix_ms(),
            details: serde_json::json!({
                "component": component,
                "rows": rows,
                "reason": reason,
                "pending_letters": pending_letters,
            }),
        }
    }

    /// Construct a `dlq_lost` event.
    pub fn dlq_lost(workflow: &str, sink: &str, letters: u64, rows: u64, reason: &str) -> Self {
        Self {
            kind: AlertEventKind::DlqLost,
            workflow: workflow.to_string(),
            node: Some(sink.to_string()),
            message: format!(
                "workflow '{workflow}' sink '{sink}' lost {letters} dead letter(s) ({rows} row(s)): {reason}"
            ),
            at_unix_ms: now_unix_ms(),
            details: serde_json::json!({
                "letters": letters,
                "rows": rows,
                "reason": reason,
            }),
        }
    }
}

/// Bounded queue capacity for asynchronous outbound alert delivery.
const MAX_ALERT_QUEUE_CAPACITY: usize = 1024;

/// Inner shared state for [`AlertDispatcher`].
type CooldownMap = Mutex<HashMap<(String, AlertEventKind, String, String), Instant>>;

struct DispatcherInner {
    config: RwLock<AlertsConfig>,
    service_label: String,
    node_id: u64,
    cooldowns: CooldownMap,
    tx: Mutex<Option<mpsc::Sender<AlertEvent>>>,
}

/// Non-blocking outbound alert dispatcher shared across runners, healers, and DLQs.
///
/// Cloning an [`AlertDispatcher`] shares the same underlying delivery queue and
/// cooldown state. [`AlertDispatcher::noop`] is a zero-cost no-op dispatcher
/// used when no alert targets are configured.
#[derive(Clone)]
pub struct AlertDispatcher {
    inner: Option<Arc<DispatcherInner>>,
}

impl std::fmt::Debug for AlertDispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlertDispatcher")
            .field("enabled", &self.inner.is_some())
            .finish()
    }
}

impl Default for AlertDispatcher {
    fn default() -> Self {
        Self::noop()
    }
}

impl AlertDispatcher {
    /// A no-op dispatcher that drops all events without allocating or spawning tasks.
    pub fn noop() -> Self {
        Self { inner: None }
    }

    /// Create an active dispatcher from `config`.
    ///
    /// Returns [`AlertDispatcher::noop`] when `config` declares no targets.
    pub fn from_config(config: &AlertsConfig, service_label: String, node_id: u64) -> Self {
        if config.is_empty() {
            return Self::noop();
        }
        Self::new_live(config.clone(), service_label, node_id)
    }

    /// Create an active dispatcher even if `config` currently has no targets (useful when
    /// live hot-reload may later populate `alerts`).
    pub fn new_live(config: AlertsConfig, service_label: String, node_id: u64) -> Self {
        let inner = Arc::new(DispatcherInner {
            config: RwLock::new(config),
            service_label,
            node_id,
            cooldowns: Mutex::new(HashMap::new()),
            tx: Mutex::new(None),
        });
        Self { inner: Some(inner) }
    }

    /// Update the active alert configuration on hot reload.
    pub fn update_config(&self, config: &AlertsConfig) {
        if let Some(inner) = &self.inner {
            *inner
                .config
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = config.clone();
        }
    }

    /// Enqueue an alert event for asynchronous delivery without blocking the caller.
    pub fn emit(&self, event: AlertEvent) {
        let Some(inner) = &self.inner else {
            return;
        };
        if inner
            .config
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
        {
            return;
        }
        let sender = {
            let mut tx_guard = inner
                .tx
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(tx) = tx_guard.as_ref()
                && !tx.is_closed()
            {
                tx.clone()
            } else if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let (tx, rx) = mpsc::channel(MAX_ALERT_QUEUE_CAPACITY);
                let worker_inner = Arc::clone(inner);
                handle.spawn(run_delivery_loop(worker_inner, rx));
                *tx_guard = Some(tx.clone());
                tx
            } else {
                // Outside a Tokio runtime (e.g. synchronous unit tests), nothing to spawn.
                return;
            }
        };
        if let Err(mpsc::error::TrySendError::Full(_)) = sender.try_send(event) {
            #[cfg(feature = "tracing")]
            tracing::warn!(
                queue_capacity = MAX_ALERT_QUEUE_CAPACITY,
                "alert delivery queue full; dropping alert event"
            );
        }
    }
}

fn target_accepts(filter: Option<&[AlertEventKind]>, kind: AlertEventKind) -> bool {
    match filter {
        None => true,
        Some(list) => list.contains(&kind),
    }
}

fn check_cooldown(
    cooldowns: &CooldownMap,
    target_id: &str,
    event: &AlertEvent,
    cooldown_ms: u64,
) -> bool {
    if cooldown_ms == 0 {
        return true;
    }
    let key = (
        target_id.to_string(),
        event.kind,
        event.workflow.clone(),
        event.node.clone().unwrap_or_default(),
    );
    let now = Instant::now();
    let window = Duration::from_millis(cooldown_ms);
    let mut map = cooldowns
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(&last) = map.get(&key)
        && now.saturating_duration_since(last) < window
    {
        return false;
    }
    map.insert(key, now);
    true
}

async fn run_delivery_loop(inner: Arc<DispatcherInner>, mut rx: mpsc::Receiver<AlertEvent>) {
    crate::service::install_ring_provider();
    let client = reqwest::Client::new();

    while let Some(event) = rx.recv().await {
        let config = inner
            .config
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if config.is_empty() {
            continue;
        }
        let timeout = Duration::from_millis(config.timeout_ms.max(1));
        let mut deliveries: Vec<futures::future::BoxFuture<'_, ()>> = Vec::new();

        for target in &config.webhooks {
            if !target_accepts(target.events.as_deref(), event.kind) {
                continue;
            }
            let cooldown = target.cooldown_ms.unwrap_or(config.cooldown_ms);
            if !check_cooldown(&inner.cooldowns, &target.id, &event, cooldown) {
                continue;
            }
            let payload = serde_json::json!({
                "event": event.kind.as_str(),
                "severity": event.kind.default_severity(),
                "service": inner.service_label,
                "node_id": inner.node_id,
                "workflow": event.workflow,
                "node": event.node,
                "message": event.message,
                "at_unix_ms": event.at_unix_ms,
                "details": event.details,
            });
            let mut req = client.post(&target.url).timeout(timeout).json(&payload);
            for (k, v) in &target.headers {
                req = req.header(k, v);
            }
            let target_id = target.id.clone();
            let target_url = target.url.clone();
            deliveries.push(Box::pin(async move {
                if let Err(_e) = send_and_check(req).await {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(target_id = %target_id, url = %target_url, error = %_e, "webhook alert delivery failed");
                }
            }));
        }

        for target in &config.slack {
            if !target_accepts(target.events.as_deref(), event.kind) {
                continue;
            }
            let cooldown = target.cooldown_ms.unwrap_or(config.cooldown_ms);
            if !check_cooldown(&inner.cooldowns, &target.id, &event, cooldown) {
                continue;
            }
            let color = match event.kind.default_severity() {
                "critical" => "#dc2626",
                "warning" => "#d97706",
                _ => "#2563eb",
            };
            let mut payload = serde_json::json!({
                "text": format!("[{}] {}", event.kind.as_str(), event.message),
                "attachments": [{
                    "color": color,
                    "title": format!("SACI Alert: {}", event.kind.as_str()),
                    "text": event.message,
                    "fields": [
                        { "title": "Service", "value": inner.service_label, "short": true },
                        { "title": "Workflow", "value": event.workflow, "short": true },
                        { "title": "Node", "value": event.node.as_deref().unwrap_or("-"), "short": true },
                    ]
                }]
            });
            if let Some(ch) = &target.channel {
                payload["channel"] = serde_json::Value::String(ch.clone());
            }
            if let Some(user) = &target.username {
                payload["username"] = serde_json::Value::String(user.clone());
            }
            let req = client
                .post(&target.webhook_url)
                .timeout(timeout)
                .json(&payload);
            let target_id = target.id.clone();
            deliveries.push(Box::pin(async move {
                if let Err(_e) = send_and_check(req).await {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(target_id = %target_id, error = %_e, "slack alert delivery failed");
                }
            }));
        }

        for target in &config.pagerduty {
            if !target_accepts(target.events.as_deref(), event.kind) {
                continue;
            }
            let cooldown = target.cooldown_ms.unwrap_or(config.cooldown_ms);
            if !check_cooldown(&inner.cooldowns, &target.id, &event, cooldown) {
                continue;
            }
            let dedup_key = format!(
                "saci/{}/{}/{}/{}",
                inner.node_id,
                event.kind.as_str(),
                event.workflow,
                event.node.as_deref().unwrap_or("_")
            );
            let payload = serde_json::json!({
                "routing_key": target.routing_key,
                "event_action": "trigger",
                "dedup_key": dedup_key,
                "payload": {
                    "summary": event.message,
                    "source": inner.service_label,
                    "severity": target.severity,
                    "custom_details": {
                        "event": event.kind.as_str(),
                        "node_id": inner.node_id,
                        "workflow": event.workflow,
                        "node": event.node,
                        "at_unix_ms": event.at_unix_ms,
                        "details": event.details,
                    }
                }
            });
            let req = client.post(&target.url).timeout(timeout).json(&payload);
            let target_id = target.id.clone();
            deliveries.push(Box::pin(async move {
                if let Err(_e) = send_and_check(req).await {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(target_id = %target_id, error = %_e, "pagerduty alert delivery failed");
                }
            }));
        }

        if !deliveries.is_empty() {
            futures::future::join_all(deliveries).await;
        }
    }
}

async fn send_and_check(req: reqwest::RequestBuilder) -> Result<(), String> {
    let resp = req.send().await.map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, routing::post};

    #[test]
    fn alerts_config_parses_and_validates_kdl() {
        let raw = r##"
mode "standalone"
node id=1 data_dir="/tmp/saci-alerts-test"

alerts cooldown_ms=5000 timeout_ms=2000 {
    webhook "ops" url="http://127.0.0.1:9999/hook" {
        header Authorization="Bearer secret"
        events "heal_exhausted" "dlq_lost"
    }
    slack "team" webhook_url="http://127.0.0.1:9999/slack" channel="#alerts"
    pagerduty "pd" routing_key="abc123" severity="critical"
}

workflow "w" {
    source "in" connector="noop" component="X"
    sink "out" connector="noop" component="X"
    link from="in" to="out"
}
"##;
        let cfg = crate::service::ServiceConfig::from_kdl(raw).expect("parse alerts");
        assert_eq!(cfg.alerts.cooldown_ms, 5000);
        assert_eq!(cfg.alerts.timeout_ms, 2000);
        assert_eq!(cfg.alerts.webhooks.len(), 1);
        assert_eq!(
            cfg.alerts.webhooks[0].events,
            Some(vec![AlertEventKind::HealExhausted, AlertEventKind::DlqLost])
        );
        assert_eq!(cfg.alerts.slack.len(), 1);
        assert_eq!(cfg.alerts.slack[0].channel.as_deref(), Some("#alerts"));
        assert_eq!(cfg.alerts.pagerduty.len(), 1);
        assert_eq!(cfg.alerts.pagerduty[0].severity, "critical");
    }

    #[tokio::test]
    async fn dispatcher_delivers_webhook_slack_pagerduty_and_enforces_cooldown() {
        let received = Arc::new(Mutex::new(Vec::<(String, serde_json::Value)>::new()));
        let rec_wh = Arc::clone(&received);
        let rec_sl = Arc::clone(&received);
        let rec_pd = Arc::clone(&received);

        let app = Router::new()
            .route(
                "/webhook",
                post(move |Json(body): Json<serde_json::Value>| {
                    let rec = Arc::clone(&rec_wh);
                    async move {
                        rec.lock().unwrap().push(("webhook".to_string(), body));
                        axum::http::StatusCode::OK
                    }
                }),
            )
            .route(
                "/slack",
                post(move |Json(body): Json<serde_json::Value>| {
                    let rec = Arc::clone(&rec_sl);
                    async move {
                        rec.lock().unwrap().push(("slack".to_string(), body));
                        axum::http::StatusCode::OK
                    }
                }),
            )
            .route(
                "/pagerduty",
                post(move |Json(body): Json<serde_json::Value>| {
                    let rec = Arc::clone(&rec_pd);
                    async move {
                        rec.lock().unwrap().push(("pagerduty".to_string(), body));
                        axum::http::StatusCode::ACCEPTED
                    }
                }),
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let config = AlertsConfig {
            cooldown_ms: 60_000,
            timeout_ms: 2_000,
            webhooks: vec![WebhookTargetSpec {
                id: "wh".to_string(),
                url: format!("http://{addr}/webhook"),
                headers: HashMap::new(),
                events: None,
                cooldown_ms: None,
            }],
            slack: vec![SlackTargetSpec {
                id: "sl".to_string(),
                webhook_url: format!("http://{addr}/slack"),
                channel: Some("#ops".to_string()),
                username: None,
                events: Some(vec![AlertEventKind::HealExhausted]),
                cooldown_ms: None,
            }],
            pagerduty: vec![PagerDutyTargetSpec {
                id: "pd".to_string(),
                routing_key: "rk-1".to_string(),
                url: format!("http://{addr}/pagerduty"),
                severity: "critical".to_string(),
                events: Some(vec![AlertEventKind::HealExhausted]),
                cooldown_ms: None,
            }],
        };

        let dispatcher = AlertDispatcher::from_config(&config, "svc-a".to_string(), 1);
        dispatcher.emit(AlertEvent::heal_exhausted(
            "orders",
            "src1",
            "source",
            3,
            Some("boom"),
        ));
        // Second identical event within cooldown must be deduplicated.
        dispatcher.emit(AlertEvent::heal_exhausted(
            "orders",
            "src1",
            "source",
            3,
            Some("boom"),
        ));
        // A different event kind is filtered out for slack/pd, delivered only to webhook.
        dispatcher.emit(AlertEvent::workflow_failed("orders", "fatal", 2));

        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while tokio::time::Instant::now() < deadline {
            if received.lock().unwrap().len() >= 4 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let items = received.lock().unwrap().clone();
        assert_eq!(
            items.len(),
            4,
            "expected 3 for heal_exhausted + 1 for workflow_failed, got {items:?}"
        );
        assert!(
            items
                .iter()
                .any(|(t, b)| t == "webhook" && b["event"] == "heal_exhausted")
        );
        assert!(
            items
                .iter()
                .any(|(t, b)| t == "slack" && b["channel"] == "#ops")
        );
        assert!(
            items
                .iter()
                .any(|(t, b)| t == "pagerduty" && b["routing_key"] == "rk-1")
        );
        assert!(
            items
                .iter()
                .any(|(t, b)| t == "webhook" && b["event"] == "workflow_failed")
        );

        server.abort();
    }

    #[tokio::test]
    async fn concurrent_delivery_is_not_blocked_by_slow_target() {
        let fast_received = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));
        let fast_rec = Arc::clone(&fast_received);

        let app = Router::new()
            .route(
                "/slow",
                post(|| async {
                    tokio::time::sleep(Duration::from_millis(600)).await;
                    axum::http::StatusCode::OK
                }),
            )
            .route(
                "/fast",
                post(move |Json(body): Json<serde_json::Value>| {
                    let rec = Arc::clone(&fast_rec);
                    async move {
                        rec.lock().unwrap().push(body);
                        axum::http::StatusCode::OK
                    }
                }),
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let config = AlertsConfig {
            cooldown_ms: 0,
            timeout_ms: 2_000,
            webhooks: vec![
                WebhookTargetSpec {
                    id: "slow".to_string(),
                    url: format!("http://{addr}/slow"),
                    headers: HashMap::new(),
                    events: None,
                    cooldown_ms: None,
                },
                WebhookTargetSpec {
                    id: "fast".to_string(),
                    url: format!("http://{addr}/fast"),
                    headers: HashMap::new(),
                    events: None,
                    cooldown_ms: None,
                },
            ],
            slack: Vec::new(),
            pagerduty: Vec::new(),
        };

        let dispatcher = AlertDispatcher::from_config(&config, "svc".to_string(), 1);
        let started = Instant::now();
        dispatcher.emit(AlertEvent::workflow_failed("wf", "boom", 1));

        let deadline = tokio::time::Instant::now() + Duration::from_millis(350);
        while tokio::time::Instant::now() < deadline {
            if !fast_received.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            fast_received.lock().unwrap().len(),
            1,
            "fast target should receive alert concurrently in <350ms despite 600ms slow target (elapsed={:?})",
            started.elapsed()
        );
        server.abort();
    }
}
