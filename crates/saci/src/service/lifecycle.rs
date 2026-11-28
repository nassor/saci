//! Per-workflow lifecycle control: `start`, `stop`, `pause`, `resume` and
//! `restart`, for standalone mode.
//!
//! Three pieces:
//!
//! - [`RunControl`] and [`PauseGate`], the runner half. [`BuiltWorkflow::run`]
//!   takes a `RunControl` (through its `RunOptions`) instead of a bare
//!   [`CancellationToken`]; `From<CancellationToken>` yields an always-open
//!   gate, so an uncontrolled call site is unchanged and allocates nothing.
//! - [`run_supervised`], which owns one workflow's runner: it builds, runs,
//!   parks, drains and rebuilds it in response to commands. With `control`
//!   `None` it is exactly one `BuiltWorkflow::run` call.
//! - [`LifecycleRegistry`], what the HTTP handlers hold: one entry per
//!   controllable workflow, each a `watch` of the published
//!   [`WorkflowStatus`] plus the channel its supervisor listens on.
//!
//! Cluster mode has none of this. `ServiceConfig::validate` allows exactly one
//! workflow there and stopping it is stopping the node, so `serve` leaves
//! `ServiceState::lifecycle` at `None` and the routes are never mounted.
//!
//! ## Desired state is process-local
//!
//! Restarting `saci-service` starts every workflow as configured: a stop or a
//! pause is not remembered across a process. `WorkflowStatus::runs` counts the
//! runners started in *this* process.

use std::cell::RefCell;
use std::sync::Arc;
use std::time::Duration;

use crate::inspector_wire::{
    BackfillReport, BackfillRequest, CursorResetReport, CursorResetRequest, ReloadReport,
    ServiceLifecycleReport, WorkflowCursorsReport, WorkflowRefusal, WorkflowRunState,
    WorkflowStatus,
};
use tokio::sync::{RwLock, mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use crate::error::SaciError;
use crate::service::alert::{AlertDispatcher, AlertEvent};
use crate::service::backfill::{apply_time_window, parameterize_workflow};
use crate::service::builder::{BuiltWorkflow, RunOptions, WorkflowFactory, rebuild_blocker};
use crate::service::config::WorkflowSpec;
use crate::service::config::cron::parse_timestamp_utc_ms;
use crate::service::redb_state::RedbStateClient;
use crate::service::standalone::WorkflowStats;

/// How long a control request waits for the transition to settle before it is
/// reported as still in progress.
///
/// A `start` builds the workflow on the calling supervisor's task, and
/// compiling a wasm component costs about 1.7 s, so the budget is well clear
/// of the one transition that is not immediate.
pub const LIFECYCLE_ACK_BUDGET: Duration = Duration::from_secs(5);

/// Unix milliseconds now, for [`WorkflowStatus::since_unix_ms`].
fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ── Runner half ──────────────────────────────────────────────────────────────

/// A runner's cancellation token plus the gate it parks on while paused.
///
/// `From<CancellationToken>` yields an always-open gate, which is why every
/// call site that does not control its runner passes a token and is unchanged.
#[derive(Clone)]
pub struct RunControl {
    /// Cancellation, exactly as before: the runner drains and returns.
    pub cancel: CancellationToken,
    /// The pause gate the runner parks on between passes.
    pub pause: PauseGate,
}

impl RunControl {
    /// Pair a cancellation token with a pause gate.
    pub fn new(cancel: CancellationToken, pause: PauseGate) -> Self {
        Self { cancel, pause }
    }
}

impl From<CancellationToken> for RunControl {
    fn from(cancel: CancellationToken) -> Self {
        Self {
            cancel,
            pause: PauseGate::open(),
        }
    }
}

impl Default for RunControl {
    /// A token nothing cancels and an always-open gate.
    fn default() -> Self {
        CancellationToken::new().into()
    }
}

/// The runner's end of one pause pair.
///
/// The two halves own opposite ends of the two channels, so dropping either
/// closes exactly one: a dropped [`PauseHandle`] ends the runner's park, and a
/// finished runner ends the supervisor's wait for an acknowledgement. Holding
/// both senders in one shared struct would keep both alive for as long as
/// either half lived, and a parked runner whose supervisor had gone would
/// never wake.
struct GateShared {
    /// Supervisor to runner: a pause is requested.
    requested: watch::Receiver<bool>,
    /// Runner to supervisor: the runner is parked.
    parked: watch::Sender<bool>,
}

/// Runner half of the pause pair.
///
/// [`PauseGate::open`] never parks and holds no channel, so an uncontrolled
/// runner allocates nothing and its pause point is one `Option` test per pass.
#[derive(Clone, Default)]
pub struct PauseGate(Option<Arc<GateShared>>);

impl PauseGate {
    /// A gate that never parks.
    pub fn open() -> Self {
        Self(None)
    }

    /// Whether a pause has been requested, whether or not the runner has
    /// parked yet.
    ///
    /// Synchronous, so a pacing sleep can cut itself short instead of waiting
    /// out a whole `every_ms` before the runner reaches its pause point.
    pub fn is_paused(&self) -> bool {
        self.0
            .as_ref()
            .is_some_and(|shared| *shared.requested.borrow())
    }

    /// Park until the pause is lifted or `cancel` fires.
    ///
    /// Returns immediately on an open gate or when no pause is requested. A
    /// dropped supervisor also releases the runner: a closed channel must
    /// never strand one.
    pub async fn park_while_paused(&self, cancel: &CancellationToken) {
        let Some(shared) = &self.0 else {
            return;
        };
        // A clone starts from the version this gate has already seen, so the
        // first `changed()` waits for the next request rather than replaying
        // the current one.
        let mut rx = shared.requested.clone();
        if !*rx.borrow_and_update() {
            return;
        }
        shared.parked.send_replace(true);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                changed = rx.changed() => {
                    // `Err` is the supervisor dropped, which releases the
                    // runner rather than stranding it.
                    if changed.is_err() || !*rx.borrow_and_update() {
                        break;
                    }
                }
            }
        }
        shared.parked.send_replace(false);
    }
}

/// Supervisor half of the pause pair.
pub struct PauseHandle {
    requested: watch::Sender<bool>,
    parked: watch::Receiver<bool>,
}

impl PauseHandle {
    /// A fresh, unpaused pair.
    pub fn new() -> (Self, PauseGate) {
        let (requested_tx, requested_rx) = watch::channel(false);
        let (parked_tx, parked_rx) = watch::channel(false);
        (
            Self {
                requested: requested_tx,
                parked: parked_rx,
            },
            PauseGate(Some(Arc::new(GateShared {
                requested: requested_rx,
                parked: parked_tx,
            }))),
        )
    }

    /// Ask the runner to park at its next pause point.
    pub fn request_pause(&self) {
        self.requested.send_replace(true);
    }

    /// Release a parked runner.
    pub fn request_resume(&self) {
        self.requested.send_replace(false);
    }

    /// Watch the runner's acknowledgement, so the supervisor can move
    /// `Pausing` to `Paused` when the runner actually parks.
    ///
    /// The receiver reports `Err` from `changed()` once the runner has
    /// finished and dropped its gate.
    pub fn parked(&self) -> watch::Receiver<bool> {
        self.parked.clone()
    }
}

// ── Commands ─────────────────────────────────────────────────────────────────

/// One lifecycle verb.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowCommand {
    /// Build and run a stopped workflow.
    Start,
    /// Drain the runner and drop the built workflow.
    Stop,
    /// Park the runner between passes, keeping every resource alive.
    Pause,
    /// Release a parked runner.
    Resume,
    /// Stop, then start, in one request.
    Restart,
}

impl WorkflowCommand {
    /// The verb as it appears in the route path.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Stop => "stop",
            Self::Pause => "pause",
            Self::Resume => "resume",
            Self::Restart => "restart",
        }
    }
}

impl std::fmt::Display for WorkflowCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Why a control request could not be served.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LifecycleError {
    /// No workflow with this id is under control.
    UnknownWorkflow(String),
    /// `start`/`stop`/`restart` on a workflow [`rebuild_blocker`] refused.
    ///
    /// [`rebuild_blocker`]: crate::service::builder::rebuild_blocker
    NotRestartable {
        /// The declared workflow id.
        id: String,
        /// Why the workflow cannot be rebuilt.
        reason: String,
    },
    /// The verb is not legal from the workflow's current state.
    IllegalTransition {
        /// The declared workflow id.
        id: String,
        /// The verb that was refused.
        command: WorkflowCommand,
        /// The state it was refused from.
        state: WorkflowRunState,
    },
    /// The supervisor is gone: its runner finished, or the process is
    /// shutting down.
    Gone(String),
    /// A parameterized backfill run failed.
    BackfillFailed(String),
    /// Inspecting or resetting source cursors failed.
    CursorError(String),
    /// Hot-reloading the service configuration failed.
    ReloadFailed(String),
    /// The requested capability is not available on this service (e.g. no store).
    Unavailable(String),
}

impl std::fmt::Display for LifecycleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownWorkflow(id) => write!(f, "no workflow '{id}' is under lifecycle control"),
            Self::NotRestartable { id, reason } => {
                write!(f, "workflow '{id}' cannot be rebuilt: {reason}")
            }
            Self::IllegalTransition { id, command, state } => write!(
                f,
                "workflow '{id}' cannot {command} from state '{}'",
                state.as_str()
            ),
            Self::Gone(id) => write!(f, "workflow '{id}' no longer has a supervisor"),
            Self::BackfillFailed(msg) => write!(f, "backfill failed: {msg}"),
            Self::CursorError(msg) => write!(f, "cursor operation failed: {msg}"),
            Self::ReloadFailed(msg) => write!(f, "reload failed: {msg}"),
            Self::Unavailable(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for LifecycleError {}

type PendingBackfill = (
    Box<(BackfillRequest, Option<WorkflowSpec>)>,
    oneshot::Sender<Result<BackfillReport, String>>,
);

/// Entry returned by [`LifecycleRegistry::standalone_entries`].
pub(crate) type StandaloneEntry = (String, WorkflowRunState, Option<Arc<RwLock<WorkflowStats>>>);

/// Live reload request delivered from the HTTP control plane to the service loop.
pub(crate) struct ReloadEnvelope {
    pub(crate) kdl: Option<String>,
    pub(crate) reply: oneshot::Sender<Result<ReloadReport, String>>,
}

/// Internal supervisor message: lifecycle verb, hot reload, or ad-hoc backfill.
enum SupervisorAction {
    Command(WorkflowCommand, oneshot::Sender<WorkflowStatus>),
    Reload(
        Box<WorkflowSpec>,
        oneshot::Sender<Result<WorkflowStatus, String>>,
    ),
    Backfill(
        Box<(BackfillRequest, Option<WorkflowSpec>)>,
        oneshot::Sender<Result<BackfillReport, String>>,
    ),
}

/// One command plus the channel its reply goes back on.
struct Envelope {
    action: SupervisorAction,
}

/// The supervisor's end of one workflow's control channels.
///
/// Produced by [`LifecycleRegistryBuilder::register`] and handed to
/// [`run_supervised`].
pub struct SupervisorChannels {
    commands: mpsc::UnboundedReceiver<Envelope>,
    status: watch::Sender<WorkflowStatus>,
    alerts: AlertDispatcher,
}

// ── Registry ─────────────────────────────────────────────────────────────────

struct Entry {
    id: String,
    status: watch::Receiver<WorkflowStatus>,
    commands: mpsc::UnboundedSender<Envelope>,
    spec: Option<WorkflowSpec>,
    live_stats: Option<Arc<RwLock<WorkflowStats>>>,
}

/// Builds a [`LifecycleRegistry`], one workflow at a time.
///
/// `serve` registers every declared workflow before it starts any runner, so
/// the HTTP control plane can answer for a workflow whose supervisor has not
/// been polled yet.
#[derive(Default)]
pub struct LifecycleRegistryBuilder {
    entries: Vec<Entry>,
    state_client: Option<Arc<RedbStateClient>>,
    reload_tx: Option<mpsc::UnboundedSender<ReloadEnvelope>>,
    alerts: AlertDispatcher,
    raw_kdl: Option<String>,
}

impl LifecycleRegistryBuilder {
    /// An empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach the persistent state client used by cursor inspect/reset routes.
    pub(crate) fn state_client(mut self, client: Option<Arc<RedbStateClient>>) -> Self {
        self.state_client = client;
        self
    }

    /// Attach the raw (pre-substitution) KDL configuration text for parameterized backfills.
    pub(crate) fn raw_kdl(mut self, raw: Option<String>) -> Self {
        self.raw_kdl = raw;
        self
    }

    /// Attach the live config reload channel handled by `serve`.
    pub(crate) fn reload_channel(mut self, tx: mpsc::UnboundedSender<ReloadEnvelope>) -> Self {
        self.reload_tx = Some(tx);
        self
    }

    /// Attach the outbound alert dispatcher shared with workflow supervisors.
    pub(crate) fn alerts(mut self, alerts: AlertDispatcher) -> Self {
        self.alerts = alerts;
        self
    }

    /// Register one workflow and return its supervisor's channels.
    ///
    /// `blocker` is [`rebuild_blocker`](crate::service::builder::rebuild_blocker)'s
    /// answer for this workflow.
    pub fn register(
        &mut self,
        id: &str,
        name: Option<&str>,
        blocker: Option<&'static str>,
    ) -> SupervisorChannels {
        self.register_with_spec(id, name, blocker, None, None)
    }

    /// Register one workflow together with its declared [`WorkflowSpec`] and
    /// live stats handle.
    pub(crate) fn register_with_spec(
        &mut self,
        id: &str,
        name: Option<&str>,
        blocker: Option<&'static str>,
        spec: Option<WorkflowSpec>,
        live_stats: Option<Arc<RwLock<WorkflowStats>>>,
    ) -> SupervisorChannels {
        let initial = WorkflowStatus {
            id: id.to_string(),
            name: name.map(str::to_string),
            state: WorkflowRunState::Starting,
            since_unix_ms: now_unix_ms(),
            runs: 0,
            restartable: blocker.is_none(),
            error: None,
            restart_blocked_reason: blocker.map(str::to_string),
        };
        let (status_tx, status_rx) = watch::channel(initial);
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        self.entries.push(Entry {
            id: id.to_string(),
            status: status_rx,
            commands: command_tx,
            spec,
            live_stats,
        });
        SupervisorChannels {
            commands: command_rx,
            status: status_tx,
            alerts: self.alerts.clone(),
        }
    }

    /// Freeze the registry.
    pub fn build(self) -> LifecycleRegistry {
        LifecycleRegistry {
            entries: std::sync::RwLock::new(self.entries),
            state_client: self.state_client,
            reload_tx: self.reload_tx,
            alerts: self.alerts,
            raw_kdl: std::sync::RwLock::new(self.raw_kdl),
        }
    }
}

/// Every controllable workflow in this process, in declaration order.
pub struct LifecycleRegistry {
    entries: std::sync::RwLock<Vec<Entry>>,
    state_client: Option<Arc<RedbStateClient>>,
    reload_tx: Option<mpsc::UnboundedSender<ReloadEnvelope>>,
    alerts: AlertDispatcher,
    raw_kdl: std::sync::RwLock<Option<String>>,
}

impl LifecycleRegistry {
    /// Replace the cached raw KDL text after a successful hot-reload.
    pub(crate) fn set_raw_kdl(&self, raw: String) {
        *self
            .raw_kdl
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(raw);
    }
    /// Every workflow's status, in declaration order.
    pub fn list(&self) -> Vec<WorkflowStatus> {
        self.entries
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|entry| entry.status.borrow().clone())
            .collect()
    }

    /// One workflow's status.
    pub fn get(&self, id: &str) -> Option<WorkflowStatus> {
        self.entries
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .find(|entry| entry.id == id)
            .map(|entry| entry.status.borrow().clone())
    }

    /// One workflow's state, for `GET /status`.
    pub fn state_of(&self, id: &str) -> Option<WorkflowRunState> {
        self.entries
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .find(|entry| entry.id == id)
            .map(|entry| entry.status.borrow().state)
    }

    /// Live stats handles and states for every registered workflow, in current order.
    pub(crate) fn standalone_entries(&self) -> Vec<StandaloneEntry> {
        self.entries
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|entry| {
                (
                    entry.id.clone(),
                    entry.status.borrow().state,
                    entry.live_stats.clone(),
                )
            })
            .collect()
    }

    /// Dynamically register a workflow added during live hot reload.
    pub(crate) fn register_dynamic(
        &self,
        spec: &WorkflowSpec,
        live_stats: Option<Arc<RwLock<WorkflowStats>>>,
    ) -> SupervisorChannels {
        let blocker = rebuild_blocker(spec);
        let initial = WorkflowStatus {
            id: spec.id.clone(),
            name: spec.name.clone(),
            state: WorkflowRunState::Starting,
            since_unix_ms: now_unix_ms(),
            runs: 0,
            restartable: blocker.is_none(),
            error: None,
            restart_blocked_reason: blocker.map(str::to_string),
        };
        let (status_tx, status_rx) = watch::channel(initial);
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        self.entries
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(Entry {
                id: spec.id.clone(),
                status: status_rx,
                commands: command_tx,
                spec: Some(spec.clone()),
                live_stats,
            });
        SupervisorChannels {
            commands: command_rx,
            status: status_tx,
            alerts: self.alerts.clone(),
        }
    }

    /// Remove a workflow deleted during live hot reload.
    pub(crate) fn remove_workflow(&self, id: &str) {
        self.entries
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|entry| entry.id != id);
    }

    /// Update the stored [`WorkflowSpec`] and send a live reload message to `id`'s supervisor.
    pub(crate) fn send_reload_workflow(
        &self,
        id: &str,
        spec: WorkflowSpec,
    ) -> Result<oneshot::Receiver<Result<WorkflowStatus, String>>, LifecycleError> {
        let mut entries = self
            .entries
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = entries
            .iter_mut()
            .find(|e| e.id == id)
            .ok_or_else(|| LifecycleError::UnknownWorkflow(id.to_string()))?;
        entry.spec = Some(spec.clone());
        let (reply_tx, reply_rx) = oneshot::channel();
        entry
            .commands
            .send(Envelope {
                action: SupervisorAction::Reload(Box::new(spec), reply_tx),
            })
            .map_err(|_| LifecycleError::Gone(id.to_string()))?;
        Ok(reply_rx)
    }

    /// Validate a verb against the workflow's current state, dispatch it, and
    /// wait up to [`LIFECYCLE_ACK_BUDGET`] for the transition to settle.
    ///
    /// The returned flag is `false` when the budget expired first, in which
    /// case the status is the latest published one and the transition is still
    /// in progress.
    ///
    /// # Errors
    ///
    /// See [`LifecycleError`]. A verb that is already satisfied is not an
    /// error: `pause` on a paused workflow and `stop` on a stopped one both
    /// return the unchanged status, settled.
    pub async fn command(
        &self,
        id: &str,
        command: WorkflowCommand,
    ) -> Result<(WorkflowStatus, bool), LifecycleError> {
        let (current, commands_tx, status_rx) = {
            let entries = self
                .entries
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let entry = entries
                .iter()
                .find(|entry| entry.id == id)
                .ok_or_else(|| LifecycleError::UnknownWorkflow(id.to_string()))?;
            (
                entry.status.borrow().clone(),
                entry.commands.clone(),
                entry.status.clone(),
            )
        };

        if !current.restartable
            && matches!(
                command,
                WorkflowCommand::Start | WorkflowCommand::Stop | WorkflowCommand::Restart
            )
        {
            return Err(LifecycleError::NotRestartable {
                id: id.to_string(),
                reason: current
                    .restart_blocked_reason
                    .unwrap_or_else(|| "the workflow cannot be rebuilt".to_string()),
            });
        }

        match disposition(command, current.state) {
            Disposition::NoOp => return Ok((current, true)),
            Disposition::Illegal => {
                return Err(LifecycleError::IllegalTransition {
                    id: id.to_string(),
                    command,
                    state: current.state,
                });
            }
            Disposition::Dispatch => {}
        }

        let (reply_tx, reply_rx) = oneshot::channel();
        commands_tx
            .send(Envelope {
                action: SupervisorAction::Command(command, reply_tx),
            })
            .map_err(|_| LifecycleError::Gone(id.to_string()))?;

        match tokio::time::timeout(LIFECYCLE_ACK_BUDGET, reply_rx).await {
            Ok(Ok(status)) => Ok((status, true)),
            // The supervisor dropped the reply without answering: it returned.
            Ok(Err(_)) => Err(LifecycleError::Gone(id.to_string())),
            Err(_) => Ok((status_rx.borrow().clone(), false)),
        }
    }

    /// Apply one verb to every controllable workflow, concurrently.
    ///
    /// Every workflow is judged on its own state, so a mixed answer is
    /// normal: pausing a service where one workflow is already paused leaves
    /// that one alone and pauses the rest. A refusal is per workflow and
    /// never stops the others, which is what makes this usable as one
    /// operator action rather than a script that has to know the current
    /// state of every workflow first.
    ///
    /// Dispatch is concurrent because each one waits up to
    /// [`LIFECYCLE_ACK_BUDGET`]; done in sequence, a service of ten workflows
    /// could take fifty seconds to answer.
    pub async fn command_all(&self, command: WorkflowCommand) -> ServiceLifecycleReport {
        let ids: Vec<String> = self
            .entries
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|entry| entry.id.clone())
            .collect();
        let outcomes = futures::future::join_all(ids.into_iter().map(|id| async move {
            let outcome = self.command(&id, command).await;
            (id, outcome)
        }))
        .await;

        let mut applied = Vec::with_capacity(outcomes.len());
        let mut refused = Vec::new();
        let mut settled = true;
        for (id, outcome) in outcomes {
            match outcome {
                Ok((status, transition_settled)) => {
                    settled &= transition_settled;
                    applied.push(status);
                }
                Err(e) => refused.push(WorkflowRefusal {
                    id,
                    error: e.to_string(),
                }),
            }
        }
        ServiceLifecycleReport {
            applied,
            refused,
            settled,
        }
    }

    /// Trigger a parameterized ad-hoc one-shot backfill for workflow `id`.
    ///
    /// # Errors
    ///
    /// Returns [`LifecycleError::UnknownWorkflow`], [`LifecycleError::Gone`],
    /// or [`LifecycleError::BackfillFailed`].
    pub async fn backfill(
        &self,
        id: &str,
        request: BackfillRequest,
    ) -> Result<BackfillReport, LifecycleError> {
        let commands_tx = {
            let entries = self
                .entries
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let entry = entries
                .iter()
                .find(|entry| entry.id == id)
                .ok_or_else(|| LifecycleError::UnknownWorkflow(id.to_string()))?;
            entry.commands.clone()
        };
        let override_spec = {
            let mut overrides: std::collections::HashMap<String, String> = request
                .params
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            if let Some(start) = &request.start_date {
                overrides.insert("start_date".to_string(), start.clone());
            }
            if let Some(end) = &request.end_date {
                overrides.insert("end_date".to_string(), end.clone());
            }
            let raw = self
                .raw_kdl
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            if !overrides.is_empty()
                && let Some(raw_text) = raw
            {
                let cfg =
                    super::config::ServiceConfig::from_kdl_with_overrides(&raw_text, &overrides)
                        .map_err(|e| LifecycleError::BackfillFailed(e.to_string()))?;
                cfg.workflows.into_iter().find(|w| w.id == id)
            } else {
                None
            }
        };

        let (reply_tx, reply_rx) = oneshot::channel();
        commands_tx
            .send(Envelope {
                action: SupervisorAction::Backfill(Box::new((request, override_spec)), reply_tx),
            })
            .map_err(|_| LifecycleError::Gone(id.to_string()))?;

        match reply_rx.await {
            Ok(Ok(report)) => Ok(report),
            Ok(Err(msg)) => Err(LifecycleError::BackfillFailed(msg)),
            Err(_) => Err(LifecycleError::Gone(id.to_string())),
        }
    }

    /// Inspect persisted source cursors, processor priors, and checkpoint history
    /// for workflow `id`.
    ///
    /// # Errors
    ///
    /// Returns [`LifecycleError::UnknownWorkflow`], [`LifecycleError::Unavailable`],
    /// or [`LifecycleError::CursorError`].
    pub async fn cursors(&self, id: &str) -> Result<WorkflowCursorsReport, LifecycleError> {
        let spec = {
            let entries = self
                .entries
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let entry = entries
                .iter()
                .find(|entry| entry.id == id)
                .ok_or_else(|| LifecycleError::UnknownWorkflow(id.to_string()))?;
            entry.spec.clone().ok_or_else(|| {
                LifecycleError::Unavailable(format!(
                    "workflow '{id}' metadata is not available for cursor inspection"
                ))
            })?
        };
        let Some(client) = &self.state_client else {
            return Ok(WorkflowCursorsReport {
                workflow: id.to_string(),
                cursors: Vec::new(),
                processors_with_state: Vec::new(),
                checkpoints_unix_ms: Vec::new(),
            });
        };
        let source_ids: Vec<String> = spec.sources.iter().map(|s| s.id.clone()).collect();
        let processor_ids: Vec<String> = processor_ids_of(&spec);
        client
            .inspect_workflow(id, &source_ids, &processor_ids)
            .await
            .map_err(|e| LifecycleError::CursorError(e.to_string()))
    }

    /// Reset or roll back source cursors and processor priors for workflow `id`.
    ///
    /// Restarts the workflow if it is currently `Running` or `Paused` so the
    /// reset offsets/priors take effect immediately.
    ///
    /// # Errors
    ///
    /// Returns [`LifecycleError::UnknownWorkflow`], [`LifecycleError::Unavailable`],
    /// or [`LifecycleError::CursorError`].
    pub async fn reset_cursors(
        &self,
        id: &str,
        request: CursorResetRequest,
    ) -> Result<CursorResetReport, LifecycleError> {
        let (spec, current_state) = {
            let entries = self
                .entries
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let entry = entries
                .iter()
                .find(|entry| entry.id == id)
                .ok_or_else(|| LifecycleError::UnknownWorkflow(id.to_string()))?;
            let spec = entry.spec.clone().ok_or_else(|| {
                LifecycleError::Unavailable(format!(
                    "workflow '{id}' metadata is not available for cursor reset"
                ))
            })?;
            (spec, entry.status.borrow().state)
        };
        let Some(client) = &self.state_client else {
            return Err(LifecycleError::Unavailable(
                "cursor reset requires a configured `store \"redb\"` block".to_string(),
            ));
        };

        let all_sources: Vec<String> = spec.sources.iter().map(|s| s.id.clone()).collect();
        let target_sources = match &request.source {
            Some(s) => {
                if !all_sources.iter().any(|known| known == s) {
                    return Err(LifecycleError::CursorError(format!(
                        "workflow '{id}' declares no source '{s}'"
                    )));
                }
                vec![s.clone()]
            }
            None => all_sources,
        };

        let to_unix_ms = match (request.to_unix_ms, request.timestamp.as_deref()) {
            (Some(ms), _) => Some(ms),
            (None, Some(ts)) => {
                let signed =
                    parse_timestamp_utc_ms(ts, false).map_err(LifecycleError::CursorError)?;
                let unsigned = u64::try_from(signed).map_err(|_| {
                    LifecycleError::CursorError(format!("timestamp '{ts}' is before Unix epoch"))
                })?;
                Some(unsigned)
            }
            (None, None) => None,
        };

        let processor_ids = processor_ids_of(&spec);
        let reset_priors = request.reset_priors.unwrap_or(true);
        let report = client
            .reset_workflow(
                id,
                &target_sources,
                &processor_ids,
                to_unix_ms,
                request.items_processed,
                reset_priors,
            )
            .await
            .map_err(|e| LifecycleError::CursorError(e.to_string()))?;

        if matches!(
            current_state,
            WorkflowRunState::Running | WorkflowRunState::Paused | WorkflowRunState::Pausing
        ) {
            let _ = self.command(id, WorkflowCommand::Restart).await;
        }
        Ok(report)
    }

    /// Trigger a live configuration and processor reload in `serve`.
    ///
    /// # Errors
    ///
    /// Returns [`LifecycleError::Unavailable`] if the service is not running
    /// with reload support, or [`LifecycleError::ReloadFailed`] if parsing,
    /// validation, or build fails.
    pub async fn reload(&self, kdl: Option<String>) -> Result<ReloadReport, LifecycleError> {
        let Some(tx) = &self.reload_tx else {
            return Err(LifecycleError::Unavailable(
                "live configuration reload is not mounted on this registry".to_string(),
            ));
        };
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(ReloadEnvelope {
            kdl,
            reply: reply_tx,
        })
        .map_err(|_| LifecycleError::ReloadFailed("service reload loop has stopped".to_string()))?;

        match reply_rx.await {
            Ok(Ok(report)) => Ok(report),
            Ok(Err(msg)) => Err(LifecycleError::ReloadFailed(msg)),
            Err(_) => Err(LifecycleError::ReloadFailed(
                "reload handler dropped without answering".to_string(),
            )),
        }
    }
}

fn processor_ids_of(spec: &WorkflowSpec) -> Vec<String> {
    spec.wasm
        .iter()
        .map(|w| w.id.clone())
        .chain(spec.plugin.iter().map(|p| p.id.clone()))
        .chain(spec.operator.iter().map(|o| o.id.clone()))
        .collect()
}

/// What [`LifecycleRegistry::command`] does with one verb.
enum Disposition {
    /// Send it to the supervisor.
    Dispatch,
    /// Already satisfied: answer the current status, unchanged.
    NoOp,
    /// Not legal from this state.
    Illegal,
}

/// The lifecycle transition table.
fn disposition(command: WorkflowCommand, state: WorkflowRunState) -> Disposition {
    use Disposition::{Dispatch, Illegal, NoOp};
    use WorkflowRunState as S;
    match command {
        WorkflowCommand::Start => match state {
            S::Stopped | S::Completed | S::Failed => Dispatch,
            S::Running => NoOp,
            _ => Illegal,
        },
        WorkflowCommand::Stop => match state {
            S::Running | S::Pausing | S::Paused => Dispatch,
            S::Stopped | S::Completed | S::Failed => NoOp,
            _ => Illegal,
        },
        WorkflowCommand::Pause => match state {
            S::Running => Dispatch,
            S::Pausing | S::Paused => NoOp,
            _ => Illegal,
        },
        WorkflowCommand::Resume => match state {
            S::Pausing | S::Paused => Dispatch,
            S::Running => NoOp,
            _ => Illegal,
        },
        WorkflowCommand::Restart => match state {
            S::Running | S::Paused | S::Stopped | S::Completed | S::Failed => Dispatch,
            _ => Illegal,
        },
    }
}

/// Whether a published state satisfies the verb that is waiting on a reply.
///
/// Terminal states satisfy every verb, so a workflow that fails or finishes
/// during a transition answers its caller instead of burning the whole
/// [`LIFECYCLE_ACK_BUDGET`].
fn target_reached(command: WorkflowCommand, state: WorkflowRunState) -> bool {
    use WorkflowRunState as S;
    match command {
        WorkflowCommand::Pause => {
            matches!(state, S::Paused | S::Stopped | S::Completed | S::Failed)
        }
        WorkflowCommand::Stop => matches!(state, S::Stopped | S::Completed | S::Failed),
        WorkflowCommand::Start | WorkflowCommand::Resume | WorkflowCommand::Restart => {
            matches!(state, S::Running | S::Completed | S::Failed)
        }
    }
}

// ── Supervisor ───────────────────────────────────────────────────────────────

/// What the supervisor does once the current runner returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AfterRun {
    /// Publish `Stopped` and park.
    Stop,
    /// Build and run again.
    Restart,
}

/// Publishes one workflow's status and answers whichever request is waiting
/// for it.
struct Publisher {
    status: Option<watch::Sender<WorkflowStatus>>,
    current: WorkflowStatus,
    pending: Option<(WorkflowCommand, oneshot::Sender<WorkflowStatus>)>,
}

impl Publisher {
    /// Move to `state`, restamping `since_unix_ms`, and settle any waiter.
    fn publish(&mut self, state: WorkflowRunState, error: Option<String>) {
        let previous = self.current.state;
        self.current.state = state;
        self.current.error = error;
        self.current.since_unix_ms = now_unix_ms();
        if let Some(tx) = &self.status {
            let _ = tx.send(self.current.clone());
        }
        tracing::info!(
            workflow = %self.current.id,
            from = previous.as_str(),
            to = state.as_str(),
            runs = self.current.runs,
            "workflow lifecycle transition"
        );
        self.settle();
    }

    /// Take over the reply channel for `command`, answering whatever was
    /// waiting before with the current status so no request is left hanging.
    ///
    /// Callers publish the transient state *first*: settling reads the current
    /// state, so accepting a `restart` while still `Running` would answer it
    /// before anything happened.
    fn accept(&mut self, command: WorkflowCommand, reply: oneshot::Sender<WorkflowStatus>) {
        if let Some((_, waiting)) = self.pending.take() {
            let _ = waiting.send(self.current.clone());
        }
        self.pending = Some((command, reply));
        self.settle();
    }

    fn settle(&mut self) {
        let reached = self
            .pending
            .as_ref()
            .is_some_and(|(command, _)| target_reached(*command, self.current.state));
        if reached && let Some((_, reply)) = self.pending.take() {
            let _ = reply.send(self.current.clone());
        }
    }
}

/// Fold one run's counters into the workflow's running total.
///
/// `BuiltWorkflow::run` counts from zero on every start, so the supervised total
/// is what survives a restart; `nodes` is the latest run's breakdown, because
/// summing per-node counters across two different builds says nothing.
fn accumulate(total: &mut WorkflowStats, run: WorkflowStats) {
    total.iterations += run.iterations;
    total.source_batches_drained += run.source_batches_drained;
    total.rows_processed += run.rows_processed;
    total.sink_batches_written += run.sink_batches_written;
    total.iteration_errors += run.iteration_errors;
    total.total_duration_ms += run.total_duration_ms;
    total.total_busy_micros += run.total_busy_micros;
    total.max_item_micros = total.max_item_micros.max(run.max_item_micros);
    total.nodes = run.nodes;
}

/// Await the next command, or never, when the workflow is not controlled.
async fn next_command(
    commands: &mut Option<mpsc::UnboundedReceiver<Envelope>>,
) -> Option<Envelope> {
    match commands {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

/// Execute a single parameterized one-shot backfill pass and fold its counters
/// into `total` and `live_stats`.
pub(crate) async fn execute_backfill(
    workflow: &WorkflowSpec,
    request: &BackfillRequest,
    factory: &RefCell<WorkflowFactory>,
    state: &Option<Arc<RedbStateClient>>,
    live_stats: &Option<Arc<RwLock<WorkflowStats>>>,
    total: &mut WorkflowStats,
    cancel: &CancellationToken,
) -> Result<BackfillReport, String> {
    let started_at_unix_ms = now_unix_ms();
    let started = std::time::Instant::now();
    let param_spec = parameterize_workflow(workflow, request).map_err(|e| e.to_string())?;
    let mut built = factory
        .borrow_mut()
        .build(&param_spec)
        .map_err(|e| e.to_string())?;
    apply_time_window(&mut built, request).map_err(|e| e.to_string())?;

    let mut options = RunOptions::new(cancel.child_token());
    if !request.reset_state
        && let Some(client) = state
    {
        options = options.state(Arc::clone(client));
    }
    let stats = built.run(options).await.map_err(|e| e.to_string())?;
    let report = BackfillReport {
        workflow: workflow.id.clone(),
        started_at_unix_ms,
        duration_ms: started.elapsed().as_millis() as u64,
        iterations: stats.iterations,
        rows_processed: stats.rows_processed,
        source_batches_drained: stats.source_batches_drained,
        sink_batches_written: stats.sink_batches_written,
        iteration_errors: stats.iteration_errors,
        start_date: request.start_date.clone(),
        end_date: request.end_date.clone(),
    };
    accumulate(total, stats);
    if let Some(shared) = live_stats {
        *shared.write().await = total.clone();
    }
    Ok(report)
}

/// Drive one workflow's runner, honouring lifecycle commands.
///
/// With `control` `None` this is exactly today's behaviour: run `initial`
/// once and return whatever the runner returned. With `Some`, the runner is
/// paused, resumed, drained and rebuilt on demand, and the function returns
/// only when `cancel` fires, when the workflow completes on its own, or when
/// it fails with no way to restart it.
///
/// `factory` is shared by every supervisor in the process: they are polled by
/// one task, so a `RefCell` is enough and the borrow is held only across the
/// synchronous [`WorkflowFactory::build`] call.
///
/// # Errors
///
/// Whatever the runner returned, when the workflow is not controlled or
/// cannot be restarted. A controlled, restartable workflow reports its failure
/// as [`WorkflowRunState::Failed`] and parks instead, so an operator can
/// restart it.
pub async fn run_supervised(
    initial: BuiltWorkflow,
    workflow: &WorkflowSpec,
    cancel: CancellationToken,
    live_stats: Option<Arc<RwLock<WorkflowStats>>>,
    state: Option<Arc<RedbStateClient>>,
    factory: &RefCell<WorkflowFactory>,
    control: Option<SupervisorChannels>,
) -> Result<WorkflowStats, SaciError> {
    let (mut commands, status, alerts) = match control {
        Some(channels) => (
            Some(channels.commands),
            Some(channels.status),
            channels.alerts,
        ),
        None => (None, None, AlertDispatcher::noop()),
    };
    let mut workflow = workflow.clone();
    let controlled = commands.is_some();
    let current = match &status {
        Some(tx) => tx.borrow().clone(),
        None => WorkflowStatus {
            id: workflow.id.clone(),
            name: workflow.name.clone(),
            state: WorkflowRunState::Starting,
            since_unix_ms: now_unix_ms(),
            runs: 0,
            restartable: false,
            error: None,
            restart_blocked_reason: None,
        },
    };
    let mut publisher = Publisher {
        status,
        current,
        pending: None,
    };

    let mut total = WorkflowStats::default();
    let mut initial = Some(initial);
    let mut run_now = true;
    let mut cancelled = false;
    let mut pending_reload: Option<oneshot::Sender<Result<WorkflowStatus, String>>> = None;
    let mut pending_backfill: Option<PendingBackfill> = None;

    loop {
        if !run_now {
            // Parked with no runner: only a command or shutdown moves us.
            tokio::select! {
                _ = cancel.cancelled() => {
                    if let Some(reply) = pending_reload.take() {
                        let _ = reply.send(Err("service shutting down".to_string()));
                    }
                    return Ok(total);
                }
                envelope = next_command(&mut commands) => match envelope {
                    Some(envelope) => match envelope.action {
                        SupervisorAction::Command(
                            cmd @ (WorkflowCommand::Start | WorkflowCommand::Restart),
                            reply,
                        ) => {
                            publisher.publish(WorkflowRunState::Starting, None);
                            publisher.accept(cmd, reply);
                            run_now = true;
                        }
                        SupervisorAction::Command(_, reply) => {
                            // The registry's transition table refuses these from a
                            // parked state; answer rather than drop the reply.
                            let _ = reply.send(publisher.current.clone());
                        }
                        SupervisorAction::Reload(new_spec, reply) => {
                            workflow = *new_spec;
                            publisher.current.name = workflow.name.clone();
                            publisher.publish(WorkflowRunState::Starting, None);
                            pending_reload = Some(reply);
                            run_now = true;
                        }
                        SupervisorAction::Backfill(payload, reply) => {
                            let prev_state = publisher.current.state;
                            let prev_error = publisher.current.error.clone();
                            publisher.publish(WorkflowRunState::Running, None);
                            let (request, override_spec) = *payload;
                            let target_spec = override_spec.as_ref().unwrap_or(&workflow);
                            let res = execute_backfill(
                                target_spec,
                                &request,
                                factory,
                                &state,
                                &live_stats,
                                &mut total,
                                &cancel,
                            )
                            .await;
                            publisher.publish(prev_state, prev_error);
                            let _ = reply.send(res);
                        }
                    },
                    // Every sender is gone: nothing can start us again.
                    None => {
                        cancel.cancelled().await;
                        return Ok(total);
                    }
                },
            }
            continue;
        }

        if let Some((payload, reply)) = pending_backfill.take() {
            publisher.publish(WorkflowRunState::Running, None);
            let (request, override_spec) = *payload;
            let target_spec = override_spec.as_ref().unwrap_or(&workflow);
            let res = execute_backfill(
                target_spec,
                &request,
                factory,
                &state,
                &live_stats,
                &mut total,
                &cancel,
            )
            .await;
            let _ = reply.send(res);
            if cancel.is_cancelled() {
                if let Some(reload_reply) = pending_reload.take() {
                    let _ = reload_reply.send(Err("service shutting down".to_string()));
                }
                return Ok(total);
            }
        }

        let built = match initial.take() {
            Some(built) => built,
            None => {
                publisher.publish(WorkflowRunState::Starting, None);
                let result = factory.borrow_mut().build(&workflow);
                match result {
                    Ok(built) => built,
                    Err(e) => {
                        let err_msg = e.to_string();
                        publisher.publish(WorkflowRunState::Failed, Some(err_msg.clone()));
                        alerts.emit(AlertEvent::workflow_failed(
                            &workflow.id,
                            &err_msg,
                            publisher.current.runs,
                        ));
                        if let Some(reply) = pending_reload.take() {
                            let _ = reply.send(Err(err_msg));
                        }
                        if controlled && publisher.current.restartable && !cancelled {
                            run_now = false;
                            continue;
                        }
                        return Err(e);
                    }
                }
            }
        };

        publisher.current.runs += 1;
        publisher.publish(WorkflowRunState::Running, None);
        if let Some(reply) = pending_reload.take() {
            let _ = reply.send(Ok(publisher.current.clone()));
        }

        let run_token = cancel.child_token();
        let (pause_handle, gate) = PauseHandle::new();
        let mut parked_rx = pause_handle.parked();
        let mut is_parked = false;
        // The runner drops its gate as it returns, closing this channel. The
        // runner arm settles the iteration either way, but a closed `watch`
        // reports ready forever, so the arm is disabled rather than spun.
        let mut parked_open = true;
        let mut after: Option<AfterRun> = None;
        let mut commands_open = controlled;

        // The runner owns the `BuiltWorkflow` and every exclusive resource its
        // connectors hold, so it is dropped at the end of this block, before
        // the next iteration can build the same workflow again.
        let outcome = {
            let mut options = RunOptions::new(RunControl::new(run_token.clone(), gate));
            if let Some(stats) = &live_stats {
                options = options.live_stats(Arc::clone(stats));
            }
            if let Some(state) = &state {
                options = options.state(Arc::clone(state));
            }
            let runner = built.run(options);
            let mut runner = std::pin::pin!(runner);
            loop {
                tokio::select! {
                    result = &mut runner => break result,

                    _ = cancel.cancelled(), if !cancelled => {
                        cancelled = true;
                        after = Some(AfterRun::Stop);
                        publisher.publish(WorkflowRunState::Stopping, None);
                        pause_handle.request_resume();
                        run_token.cancel();
                    }

                    changed = parked_rx.changed(), if parked_open => {
                        if changed.is_err() {
                            parked_open = false;
                        } else {
                            is_parked = *parked_rx.borrow_and_update();
                            if is_parked {
                                if publisher.current.state == WorkflowRunState::Pausing {
                                    publisher.publish(WorkflowRunState::Paused, None);
                                }
                            } else if publisher.current.state == WorkflowRunState::Paused {
                                publisher.publish(WorkflowRunState::Running, None);
                            }
                        }
                    }

                    envelope = next_command(&mut commands), if commands_open => {
                        match envelope {
                            None => commands_open = false,
                            Some(envelope) => match envelope.action {
                                SupervisorAction::Command(WorkflowCommand::Pause, reply) => {
                                    if !matches!(
                                        publisher.current.state,
                                        WorkflowRunState::Pausing | WorkflowRunState::Paused
                                    ) {
                                        pause_handle.request_pause();
                                        publisher.publish(WorkflowRunState::Pausing, None);
                                    }
                                    publisher.accept(WorkflowCommand::Pause, reply);
                                }
                                SupervisorAction::Command(WorkflowCommand::Resume, reply) => {
                                    pause_handle.request_resume();
                                    // A runner that never reached its pause
                                    // point will never report unparking, so
                                    // the supervisor closes the transition
                                    // itself.
                                    if !is_parked
                                        && matches!(
                                            publisher.current.state,
                                            WorkflowRunState::Pausing | WorkflowRunState::Paused
                                        )
                                    {
                                        publisher.publish(WorkflowRunState::Running, None);
                                    }
                                    publisher.accept(WorkflowCommand::Resume, reply);
                                }
                                SupervisorAction::Command(
                                    cmd @ (WorkflowCommand::Stop | WorkflowCommand::Restart),
                                    reply,
                                ) => {
                                    after = Some(if cmd == WorkflowCommand::Stop {
                                        AfterRun::Stop
                                    } else {
                                        AfterRun::Restart
                                    });
                                    publisher.publish(WorkflowRunState::Stopping, None);
                                    publisher.accept(cmd, reply);
                                    pause_handle.request_resume();
                                    run_token.cancel();
                                }
                                SupervisorAction::Command(WorkflowCommand::Start, reply) => {
                                    let _ = reply.send(publisher.current.clone());
                                }
                                SupervisorAction::Reload(new_spec, reply) => {
                                    workflow = *new_spec;
                                    publisher.current.name = workflow.name.clone();
                                    after = Some(AfterRun::Restart);
                                    pending_reload = Some(reply);
                                    publisher.publish(WorkflowRunState::Stopping, None);
                                    pause_handle.request_resume();
                                    run_token.cancel();
                                }
                                SupervisorAction::Backfill(payload, reply) => {
                                    if pending_backfill.is_some() {
                                        let _ = reply.send(Err(
                                            "a backfill is already pending for this workflow"
                                                .to_string(),
                                        ));
                                    } else {
                                        after = Some(AfterRun::Restart);
                                        pending_backfill = Some((payload, reply));
                                        publisher.publish(WorkflowRunState::Stopping, None);
                                        pause_handle.request_resume();
                                        run_token.cancel();
                                    }
                                }
                            },
                        }
                    }
                }
            }
        };

        // A shutdown that arrived while the runner was already returning may
        // never have been selected, so the token, not the flag, is what says
        // whether this exit was a stop.
        cancelled |= cancel.is_cancelled();

        match outcome {
            Ok(stats) => {
                accumulate(&mut total, stats);
                if cancelled {
                    if let Some((payload, reply)) = pending_backfill.take() {
                        let _ = reply.send(Err("service shutting down".to_string()));
                        let _ = payload;
                    }
                    if let Some(reply) = pending_reload.take() {
                        let _ = reply.send(Err("service shutting down".to_string()));
                    }
                    publisher.publish(WorkflowRunState::Stopped, None);
                    return Ok(total);
                }
                match after {
                    Some(AfterRun::Restart) => {}
                    Some(AfterRun::Stop) => {
                        publisher.publish(WorkflowRunState::Stopped, None);
                        run_now = false;
                    }
                    // The runner finished its own work. Returning here is what
                    // lets a one-shot service exit when every workflow is done.
                    None => {
                        publisher.publish(WorkflowRunState::Completed, None);
                        return Ok(total);
                    }
                }
            }
            Err(e) => {
                let err_msg = e.to_string();
                publisher.publish(WorkflowRunState::Failed, Some(err_msg.clone()));
                alerts.emit(AlertEvent::workflow_failed(
                    &workflow.id,
                    &err_msg,
                    publisher.current.runs,
                ));
                if let Some(reply) = pending_reload.take() {
                    let _ = reply.send(Err(err_msg));
                }
                if cancelled || !controlled || !publisher.current.restartable {
                    return Err(e);
                }
                run_now = false;
            }
        }
    }
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn an_open_gate_never_parks() {
        let gate = PauseGate::open();
        let cancel = CancellationToken::new();
        assert!(!gate.is_paused());
        // Would hang if the gate parked.
        gate.park_while_paused(&cancel).await;
    }

    #[tokio::test]
    async fn a_gate_parks_until_resumed_and_reports_it() {
        let (handle, gate) = PauseHandle::new();
        let cancel = CancellationToken::new();
        let mut parked = handle.parked();

        handle.request_pause();
        assert!(gate.is_paused());

        let gate_clone = gate.clone();
        let cancel_clone = cancel.clone();
        let parked_task = tokio::spawn(async move {
            gate_clone.park_while_paused(&cancel_clone).await;
        });

        parked.changed().await.expect("parked signal");
        assert!(*parked.borrow_and_update());
        assert!(!parked_task.is_finished());

        handle.request_resume();
        parked_task.await.expect("gate released");
        assert!(!gate.is_paused());
    }

    #[tokio::test]
    async fn a_cancelled_gate_releases_a_parked_runner() {
        let (handle, gate) = PauseHandle::new();
        let cancel = CancellationToken::new();
        handle.request_pause();

        let gate_clone = gate.clone();
        let cancel_clone = cancel.clone();
        let task = tokio::spawn(async move {
            gate_clone.park_while_paused(&cancel_clone).await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!task.is_finished());

        cancel.cancel();
        task.await.expect("cancel released the gate");
    }

    #[tokio::test]
    async fn a_dropped_supervisor_releases_a_parked_runner() {
        let (handle, gate) = PauseHandle::new();
        let cancel = CancellationToken::new();
        handle.request_pause();

        let task = tokio::spawn(async move {
            gate.park_while_paused(&cancel).await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(handle);
        task.await.expect("dropping the handle released the gate");
    }

    #[test]
    fn the_transition_table_matches_the_documented_verbs() {
        use WorkflowRunState as S;
        assert!(matches!(
            disposition(WorkflowCommand::Start, S::Stopped),
            Disposition::Dispatch
        ));
        assert!(matches!(
            disposition(WorkflowCommand::Start, S::Running),
            Disposition::NoOp
        ));
        assert!(matches!(
            disposition(WorkflowCommand::Pause, S::Stopped),
            Disposition::Illegal
        ));
        assert!(matches!(
            disposition(WorkflowCommand::Resume, S::Paused),
            Disposition::Dispatch
        ));
        assert!(matches!(
            disposition(WorkflowCommand::Restart, S::Stopping),
            Disposition::Illegal
        ));
        assert!(matches!(
            disposition(WorkflowCommand::Stop, S::Completed),
            Disposition::NoOp
        ));
    }

    #[tokio::test]
    async fn an_unknown_workflow_is_reported_as_such() {
        let registry = LifecycleRegistryBuilder::new().build();
        let err = registry
            .command("nope", WorkflowCommand::Stop)
            .await
            .expect_err("no such workflow");
        assert_eq!(err, LifecycleError::UnknownWorkflow("nope".to_string()));
    }

    #[tokio::test]
    async fn a_blocked_workflow_refuses_the_rebuild_verbs_but_allows_pause() {
        let mut builder = LifecycleRegistryBuilder::new();
        let channels = builder.register("w", None, Some("channel bridge"));
        let registry = builder.build();

        let err = registry
            .command("w", WorkflowCommand::Restart)
            .await
            .expect_err("not restartable");
        assert!(matches!(err, LifecycleError::NotRestartable { .. }));

        // Registered workflows start in `Starting`, which refuses `pause`; the
        // point here is that the verb is judged on state, not restartability.
        let err = registry
            .command("w", WorkflowCommand::Pause)
            .await
            .expect_err("not running yet");
        assert!(matches!(err, LifecycleError::IllegalTransition { .. }));
        drop(channels);
    }

    #[tokio::test]
    async fn a_dispatched_command_with_no_supervisor_is_gone() {
        let mut builder = LifecycleRegistryBuilder::new();
        let SupervisorChannels {
            commands, status, ..
        } = builder.register("w", None, None);
        // Publish `Running` so `pause` dispatches, then drop the receiver: a
        // supervisor that returned is exactly this shape.
        status.send_replace(WorkflowStatus {
            id: "w".to_string(),
            name: None,
            state: WorkflowRunState::Running,
            since_unix_ms: 0,
            runs: 1,
            restartable: true,
            error: None,
            restart_blocked_reason: None,
        });
        drop(commands);
        let registry = builder.build();

        let err = registry
            .command("w", WorkflowCommand::Pause)
            .await
            .expect_err("supervisor gone");
        assert_eq!(err, LifecycleError::Gone("w".to_string()));
        drop(status);
    }
}
