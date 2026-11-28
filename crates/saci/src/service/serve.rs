//! [`serve`]: run a [`ServiceConfig`] as a service until it is told to stop.
//!
//! `serve` initialises logging and OpenTelemetry, builds every workflow from
//! the registered factories, wires the HTTP control plane and watchdog, then
//! dispatches to the standalone or cluster runner. It waits for SIGINT,
//! SIGTERM or [`ServeOptions::shutdown`], then drains all tasks within the
//! 30-second shutdown budget.
//!
//! The `ready` flag flips as soon as the runner is spawned, not after the first
//! successful pipeline iteration.
//!
//! The `saci-service serve` subcommand is a thin wrapper: it loads the config,
//! applies command-line overrides and calls [`serve`]. An application that
//! embeds the service calls it directly with its own [`ServiceBuilder`].

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant, SystemTime};

use futures::future::LocalBoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use opentelemetry_prometheus::exporter;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::inspector_wire::{ReloadReport, WorkflowTopology};
use crate::service::RedbStateClient;
use crate::service::alert::AlertDispatcher;
use crate::service::builder::{BuiltWorkflow, WorkflowFactory, rebuild_blocker};
use crate::service::config::{ServiceConfig, ServiceMode, WorkflowSpec};
use crate::service::dlq::DlqRegistry;
use crate::service::http::{ServiceModeLabel, ServiceState};
use crate::service::lifecycle::{
    LifecycleRegistry, LifecycleRegistryBuilder, ReloadEnvelope, SupervisorChannels, run_supervised,
};
use crate::service::standalone::WorkflowStats;
use crate::service::topology::{update_topology_from_config, workflow_topology};
use crate::service::{ServiceBuilder, ShutdownCoordinator, serve_http, spawn_watchdog};
use crate::{SaciError, SaciResult};

/// Total time a stop gets, shared by its two stages: each runner's own
/// cancellation drain (flush staged batches, `finish()` every sink, report
/// where flow control ended), then the HTTP and watchdog tasks. Overrunning it
/// returns [`ServeExit::ShutdownBudgetExceeded`] without printing the
/// clean-stop line.
const SHUTDOWN_BUDGET: Duration = Duration::from_secs(30);

/// How [`serve`] reports itself and what it persists.
#[derive(Debug, Clone, Default)]
pub struct ServeOptions {
    /// The config file, when there is one: with a `store` block configured its
    /// raw bytes (before variable substitution, so `${VAR}` secrets stay
    /// references) are stored beside the redb state. `None` stores nothing.
    pub config_path: Option<PathBuf>,
    /// Print the three stdout lines `serve` owns: `saci-service listening on
    /// ..`, the dashboard or endpoints line, and `saci-service stopped
    /// cleanly`.
    pub announce: bool,
    /// Stops the service when cancelled, as SIGINT or SIGTERM do. `None`
    /// leaves the OS signals as the only way to stop it.
    pub shutdown: Option<CancellationToken>,
}

/// How a [`serve`] call that stopped without an error ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServeExit {
    /// Every runner, the HTTP server and the watchdog drained inside the
    /// shutdown budget.
    Clean,
    /// The 30-second shutdown budget ran out before the runners or the
    /// background tasks finished draining. A binary exits non-zero on this.
    ShutdownBudgetExceeded,
}

/// Runs `config` as a service until a shutdown signal, [`ServeOptions::shutdown`]
/// or a runner exit stops it.
///
/// `builder` carries the factories the workflows are built from; `serve`
/// applies the dead letter queue registry and the inspector to it. Every
/// workflow is built before the HTTP port is bound, so a bad config is a
/// startup failure.
///
/// Installs the process-wide tracing subscriber and OpenTelemetry meter
/// provider, so it runs at most once per process.
///
/// # Errors
///
/// Logging or store initialisation failures, a workflow that does not build,
/// an invalid HTTP bind address, or whatever error the runners returned.
pub async fn serve(
    mut config: ServiceConfig,
    builder: ServiceBuilder,
    options: ServeOptions,
) -> SaciResult<ServeExit> {
    let ServeOptions {
        config_path,
        announce,
        shutdown,
    } = options;

    // Logging must be initialised before any tracing call. The inspector is
    // built here too, because its capture layer joins the same subscriber.
    let (telemetry, inspector) =
        crate::service::init_logging(&config.observability, config.node.id)?;
    tracing::info!(node_id = config.node.id, "saci-service starting");
    // With a `store` block configured, persist the raw config file (pre
    // env-substitution, so `${VAR}` secrets stay as references) before the
    // pipeline builds; an unreachable store fails startup here rather than
    // mid-run. `validate` and `cluster init` do not write.
    let state_client: Option<Arc<RedbStateClient>> = match &config.store {
        Some(crate::service::config::StoreConfig::Redb { path, .. }) => {
            let client = Arc::new(RedbStateClient::open(path)?);
            if let Some(config_path) = &config_path {
                let raw = std::fs::read(config_path).map_err(|e| {
                    SaciError::configuration(format!(
                        "reading config file {}: {e}",
                        config_path.display()
                    ))
                })?;
                let name = config_path
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_else(|| "saci.kdl".to_string());
                client.put_config(&name, &raw).await?;
                tracing::info!(config = %name, store = %path.display(), "pipeline config persisted");
            }
            Some(client)
        }
        None => None,
    };

    let prometheus_registry = Arc::new(prometheus::Registry::new());
    let otel_exporter = exporter()
        // Instrument names already carry the `_total` convention, so the
        // exporter must not append a second one.
        .without_counter_suffixes()
        .with_registry((*prometheus_registry).clone())
        .build()
        .map_err(|e| SaciError::generic(format!("failed to build OTel exporter: {e}")))?;
    // Both readers live on one provider: `with_reader` is additive and each
    // reader gets its own aggregation pipeline, so the inspector's cumulative
    // temporality cannot disturb the Prometheus reader's.
    let mut provider_builder = SdkMeterProvider::builder().with_reader(otel_exporter);
    if let Some(inspector) = &inspector {
        provider_builder = provider_builder.with_reader(
            PeriodicReader::builder(inspector.metric_exporter())
                .with_interval(config.observability.inspector.sample_interval())
                .build(),
        );
    }
    // `metrics::init()` forces the instrument LazyLock, so it must follow the
    // provider install: instruments bind to whichever provider is current when
    // they are first built.
    opentelemetry::global::set_meter_provider(provider_builder.build());
    crate::metrics::init();

    // Alert dispatcher shared across self-healing connectors, DLQs, and workflow
    // supervisors. Updated dynamically when config reloads.
    let alerts =
        AlertDispatcher::new_live(config.alerts.clone(), config.node.label(), config.node.id);
    let lineage = Arc::new(crate::service::lineage::LineageDispatcher::new(
        &config, None,
    ));
    let builder = builder.alerts(alerts.clone()).lineage(lineage.clone());

    // One shared half per declared queue, built before the workflows so the
    // runner and the control plane are two views of the same state.
    let dlq_registry = Arc::new(DlqRegistry::from_config(&config));
    let builder = builder.dlq_registry(dlq_registry.clone());
    // The builder publishes the topology into the inspector once it knows the
    // runtime and the source/sink sets.
    let builder = match inspector.clone() {
        Some(inspector) => builder.inspector(inspector),
        None => builder,
    };
    // Building every workflow up front keeps a bad config a startup failure,
    // before the port is bound. The factory outlives the build so the
    // lifecycle plane's `start` and `restart` can build one workflow again;
    // `build_one` validates each workflow's graph (rule: matching components
    // and field-for-field identical Arrow schemas end to end), so a
    // config/runtime mismatch fails here rather than on the first iteration.
    let mut factory = builder.into_factory(&config);
    let mut built = Vec::with_capacity(config.workflows.len());
    for workflow in &config.workflows {
        built.push(factory.build(workflow)?);
    }
    factory.publish_topology(&config, &built);
    factory.publish_lineage(&config, &built);

    let coord = ShutdownCoordinator::new(SHUTDOWN_BUDGET);
    let coord_root = coord.root();

    let liveness = Arc::new(AtomicU64::new(0));
    let ready = Arc::new(AtomicBool::new(false));
    let mode_label = match &config.mode {
        ServiceMode::Standalone { .. } => ServiceModeLabel::Standalone,
        ServiceMode::Cluster { .. } => ServiceModeLabel::Cluster,
    };
    let standalone_stats: Option<Vec<(String, Arc<RwLock<WorkflowStats>>)>> = match &config.mode {
        ServiceMode::Standalone { .. } => Some(
            built
                .iter()
                .map(|b| {
                    (
                        b.workflow_id.clone(),
                        Arc::new(RwLock::new(WorkflowStats::default())),
                    )
                })
                .collect(),
        ),
        ServiceMode::Cluster { .. } => None,
    };

    let (reload_tx, mut reload_rx) = tokio::sync::mpsc::unbounded_channel::<ReloadEnvelope>();
    let reload_tx_keepalive = reload_tx.clone();

    let initial_raw_kdl = config_path
        .as_ref()
        .and_then(|p| std::fs::read_to_string(p).ok());

    // Lifecycle control is standalone-only: `ServiceConfig::validate` allows
    // exactly one workflow in cluster mode and stopping it is stopping the
    // node. With no registry the routes are never mounted.
    let control_enabled =
        matches!(config.mode, ServiceMode::Standalone { .. }) && config.http.control;
    let mut lifecycle_builder = LifecycleRegistryBuilder::new()
        .state_client(state_client.clone())
        .reload_channel(reload_tx)
        .alerts(alerts.clone())
        .raw_kdl(initial_raw_kdl);
    let supervisor_channels: Vec<Option<SupervisorChannels>> = config
        .workflows
        .iter()
        .enumerate()
        .map(|(i, workflow)| {
            control_enabled.then(|| {
                lifecycle_builder.register_with_spec(
                    &workflow.id,
                    workflow.name.as_deref(),
                    rebuild_blocker(workflow),
                    Some(workflow.clone()),
                    standalone_stats
                        .as_ref()
                        .and_then(|s| s.get(i).map(|(_, lock)| Arc::clone(lock))),
                )
            })
        })
        .collect();
    let lifecycle = control_enabled.then(|| Arc::new(lifecycle_builder.build()));

    let state = ServiceState {
        node_id: config.node.id,
        node_name: config.node.name.clone(),
        mode: mode_label,
        started_at: Instant::now(),
        prometheus_registry,
        liveness: liveness.clone(),
        ready: ready.clone(),
        standalone_stats: standalone_stats.clone(),
        inspector,
        lifecycle: lifecycle.clone(),
        dlq: if matches!(config.mode, ServiceMode::Standalone { .. }) {
            Some(dlq_registry.clone())
        } else {
            (!dlq_registry.is_empty()).then_some(dlq_registry.clone())
        },
        lineage: Some(lineage.clone()),
    };

    let watchdog_handle = spawn_watchdog(state.clone(), coord.child());

    // Resolve the bind address and print it so test harnesses can read the
    // port. For port 0 we pre-bind to learn the OS-assigned port, drop the
    // temporary listener, and let serve_http rebind. The gap is safe because
    // the OS does not hand out ephemeral ports in LIFO order.
    let http_bind_addr: std::net::SocketAddr = config.http.bind.parse().map_err(|e| {
        SaciError::configuration(format!(
            "invalid HTTP bind address '{}': {e}",
            config.http.bind
        ))
    })?;
    let resolved_addr = if http_bind_addr.port() == 0 {
        let tmp = tokio::net::TcpListener::bind(http_bind_addr)
            .await
            .map_err(|e| {
                SaciError::generic(format!(
                    "failed to probe HTTP bind address {http_bind_addr}: {e}"
                ))
            })?;
        let addr = tmp
            .local_addr()
            .map_err(|e| SaciError::generic(format!("failed to read local address: {e}")))?;
        drop(tmp);
        // Update config so serve_http binds the same concrete port.
        config.http.bind = addr.to_string();
        addr
    } else {
        http_bind_addr
    };
    if announce {
        println!("saci-service listening on {resolved_addr}");
        if state.inspector.as_ref().is_some_and(|i| i.ui_enabled()) {
            println!("dashboard at http://{resolved_addr}/ui");
        } else {
            println!("endpoints at http://{resolved_addr}/");
        }
    }

    let http_config = config.http.clone();
    let http_state = state.clone();
    let http_cancel = coord.child();
    let http_handle = tokio::spawn(async move {
        if let Err(e) = serve_http(&http_config, http_state, http_cancel).await {
            tracing::error!(error = %e, "http server failed");
        }
    });

    // Ready flips once the runner is spawned, not after the first pipeline
    // iteration completes.
    ready.store(true, Ordering::Relaxed);

    // The runner runs inline rather than under tokio::spawn: BuiltWorkflow holds
    // a Box<dyn Sink> that is Send but not Sync, and running inline avoids the
    // Future: Send bound.
    //
    // One cancel-child token per built workflow, taken before `coord` itself
    // is consumed by `wait_for_signal()` below. This works uniformly for
    // standalone mode (N workflows) and cluster mode (exactly one, guaranteed
    // by `ServiceConfig::validate`).
    let items: Vec<_> = built
        .into_iter()
        .zip(supervisor_channels)
        .map(|(b, channels)| {
            let cancel_child = coord.child();
            (b, cancel_child, channels)
        })
        .collect();
    // Every supervisor shares one factory: they are polled on this single task,
    // so a `RefCell` is enough and the `borrow_mut` never spans an await.
    let factory = RefCell::new(factory);
    let initial_config = config.clone();
    let runner_stats = standalone_stats.clone();
    let runner_config_path = config_path.clone();

    let runner_fut = async move {
        let _reload_tx_keepalive = reload_tx_keepalive;
        match initial_config.mode {
            ServiceMode::Standalone { .. } => {
                run_standalone_with_reload(
                    initial_config,
                    items,
                    runner_stats,
                    state_client,
                    &factory,
                    lifecycle,
                    dlq_registry,
                    alerts,
                    lineage,
                    coord_root,
                    runner_config_path,
                    &mut reload_rx,
                )
                .await
            }
            #[cfg(feature = "service-cluster")]
            ServiceMode::Cluster { .. } => {
                drop(lineage);
                let (b, cancel_child, _channels) = items
                    .into_iter()
                    .next()
                    .expect("cluster mode config validation guarantees exactly one workflow");
                crate::service::run_cluster(b, &initial_config, cancel_child)
                    .await
                    .map(|_| ())
            }
            // `service`-only build: the config parses, so the refusal names
            // the feature instead of reporting `mode` as an unknown value.
            // `validate_build_capabilities` already refuses this config
            // before `serve` is called from the CLI, so reaching here would
            // mean the config's mode changed underneath us; the arm exists to
            // keep the match exhaustive and answers identically either way.
            #[cfg(not(feature = "service-cluster"))]
            ServiceMode::Cluster { .. } => {
                // Drop the runner-only inputs explicitly so the async move
                // closure still captures them.
                drop(items);
                drop(runner_stats);
                drop(factory);
                drop(lifecycle);
                drop(dlq_registry);
                drop(alerts);
                drop(lineage);
                drop(coord_root);
                drop(runner_config_path);
                let _ = &mut reload_rx;
                Err(crate::service::factories::missing_cluster_host_error())
            }
        }
    };

    // `wait_for_signal` consumes `coord` and cancels the root token itself;
    // clone the token first so the other paths can do the same.
    let shutdown_token = coord.root();
    let requested = shutdown.unwrap_or_default();

    // Which of the two happened first. The runner future is pinned rather than
    // moved into the `select!` so the signal path can go on awaiting it: it is
    // what runs each runner's own cancellation drain, flushing staged batches,
    // calling `finish()` on every sink, and reporting where flow control
    // ended. Dropping it here, as a bare `select!` arm would, abandons all
    // three.
    enum FirstEvent {
        Signal,
        RunnerExit(Result<(), SaciError>),
    }

    let mut runner_fut = std::pin::pin!(runner_fut);

    let first = tokio::select! {
        _ = coord.wait_for_signal() => FirstEvent::Signal,
        _ = requested.cancelled() => FirstEvent::Signal,
        result = &mut runner_fut => FirstEvent::RunnerExit(result),
    };

    // Shutdown starts here, whichever event fired. One deadline covers both
    // drain stages, the runners and then the tasks, so a stop costs at most
    // `SHUTDOWN_BUDGET` however the two split it.
    let deadline = tokio::time::Instant::now() + SHUTDOWN_BUDGET;

    let runner_result = match first {
        FirstEvent::RunnerExit(result) => {
            tracing::info!("runner exited before shutdown signal; initiating shutdown");
            result
        }
        FirstEvent::Signal => {
            tracing::info!("shutdown signal received");
            // An OS signal already cancelled the root token inside
            // `wait_for_signal`; a request through `ServeOptions::shutdown`
            // has not. Every runner polls a child of the root, so this await
            // is the drain and not a wait for new work.
            shutdown_token.cancel();
            match tokio::time::timeout_at(deadline, &mut runner_fut).await {
                Ok(result) => result,
                Err(_) => {
                    // Same contract as the task drain below: a stop that ran
                    // out of budget was not a drain, so it must not print the
                    // line that says it was.
                    tracing::error!(
                        budget_secs = SHUTDOWN_BUDGET.as_secs(),
                        "shutdown budget exceeded draining runners; forcing exit"
                    );
                    return Ok(ServeExit::ShutdownBudgetExceeded);
                }
            }
        }
    };

    // Idempotent: cancels the HTTP and watchdog child tokens on both paths.
    shutdown_token.cancel();

    if let Err(e) = &runner_result {
        tracing::error!(error = %e, "runner exited with error");
    }

    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    let drain_coord = ShutdownCoordinator::new(remaining);
    let clean = drain_coord.drain(vec![http_handle, watchdog_handle]).await;
    if !clean {
        tracing::error!("shutdown budget exceeded; forcing exit");
        return Ok(ServeExit::ShutdownBudgetExceeded);
    }
    // Flush spans while no task is still emitting, then print the last line.
    telemetry.shutdown().await;
    if announce {
        // `println!`, not `tracing::info!`: this line is the documented proof
        // that a stop was a drain, and the default `log_level="error"` would
        // swallow an event. It pairs with the startup banner, which is a
        // `println!` for the same reason.
        println!("saci-service stopped cleanly");
    }

    // Cancellation from a clean shutdown signal is not an error.
    runner_result.map(|()| ServeExit::Clean)
}

struct ActiveWorkflow {
    spec: WorkflowSpec,
    artifact_mtimes: Vec<(String, Option<u64>, Option<SystemTime>)>,
    cancel: CancellationToken,
}

fn artifact_stat(
    path: &str,
    base_dir: Option<&std::path::Path>,
) -> (Option<u64>, Option<SystemTime>) {
    let p = std::path::Path::new(path);
    let meta = std::fs::metadata(p).or_else(|e| {
        if p.is_relative()
            && let Some(dir) = base_dir
        {
            std::fs::metadata(dir.join(p))
        } else {
            Err(e)
        }
    });
    match meta {
        Ok(m) => (Some(m.len()), m.modified().ok()),
        Err(_) => (None, None),
    }
}

fn workflow_artifact_mtimes(
    spec: &WorkflowSpec,
    base_dir: Option<&std::path::Path>,
) -> Vec<(String, Option<u64>, Option<SystemTime>)> {
    let mut out = Vec::new();
    for w in &spec.wasm {
        if let Some(path) = &w.path {
            let (len, mtime) = artifact_stat(path, base_dir);
            out.push((format!("wasm:{}:{path}", w.id), len, mtime));
        }
    }
    for p in &spec.plugin {
        if let Some(path) = &p.path {
            let (len, mtime) = artifact_stat(path, base_dir);
            out.push((format!("plugin:{}:{path}", p.id), len, mtime));
        }
    }
    out
}

fn global_build_settings_differ(old: &ServiceConfig, new: &ServiceConfig) -> bool {
    let old_run = match &old.mode {
        ServiceMode::Standalone { config } => &config.run,
        ServiceMode::Cluster { .. } => return true,
    };
    let new_run = match &new.mode {
        ServiceMode::Standalone { config } => &config.run,
        ServiceMode::Cluster { .. } => return true,
    };
    old_run != new_run
        || old.flow_control != new.flow_control
        || old.heal != new.heal
        || old.encryption != new.encryption
        || serde_json::to_value(&old.store).ok() != serde_json::to_value(&new.store).ok()
}

fn specs_equal(a: &WorkflowSpec, b: &WorkflowSpec) -> bool {
    serde_json::to_value(a).ok() == serde_json::to_value(b).ok()
}

fn spawn_workflow<'a>(
    built: BuiltWorkflow,
    spec: WorkflowSpec,
    cancel: CancellationToken,
    stats: Option<Arc<RwLock<WorkflowStats>>>,
    state: Option<Arc<RedbStateClient>>,
    factory: &'a RefCell<WorkflowFactory>,
    channels: Option<SupervisorChannels>,
) -> LocalBoxFuture<'a, (String, Result<(), SaciError>)> {
    let id = spec.id.clone();
    Box::pin(async move {
        let res = run_supervised(built, &spec, cancel, stats, state, factory, channels)
            .await
            .map(|_| ());
        (id, res)
    })
}

async fn next_sighup(
    #[cfg(unix)] sig: &mut tokio::signal::unix::Signal,
    #[cfg(not(unix))] _: &mut (),
) {
    #[cfg(unix)]
    {
        sig.recv().await;
    }
    #[cfg(not(unix))]
    {
        std::future::pending::<()>().await;
    }
}

async fn drain_workflow_ids<'a>(
    active_futs: &mut FuturesUnordered<LocalBoxFuture<'a, (String, Result<(), SaciError>)>>,
    active: &mut HashMap<String, ActiveWorkflow>,
    mut pending_ids: HashSet<String>,
) -> Result<(), SaciError> {
    let deadline = tokio::time::Instant::now() + SHUTDOWN_BUDGET;
    while !pending_ids.is_empty() {
        let next_item = tokio::time::timeout_at(deadline, active_futs.next())
            .await
            .map_err(|_| {
                SaciError::generic(format!(
                    "timed out waiting for workflows {:?} to stop during reload",
                    pending_ids
                ))
            })?;
        match next_item {
            Some((id, res)) => {
                active.remove(&id);
                if pending_ids.remove(&id) {
                    let _ = res;
                } else {
                    res?;
                }
            }
            None => break,
        }
    }
    Ok(())
}

async fn wait_reload_replies<'a>(
    active_futs: &mut FuturesUnordered<LocalBoxFuture<'a, (String, Result<(), SaciError>)>>,
    active: &mut HashMap<String, ActiveWorkflow>,
    pending: Vec<(
        String,
        tokio::sync::oneshot::Receiver<Result<crate::inspector_wire::WorkflowStatus, String>>,
    )>,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + SHUTDOWN_BUDGET;
    for (id, mut rx) in pending {
        loop {
            tokio::select! {
                reply = &mut rx => {
                    match reply {
                        Ok(Ok(_status)) => break,
                        Ok(Err(err)) => return Err(format!("workflow '{id}' reload failed: {err}")),
                        Err(_) => return Err(format!("workflow '{id}' supervisor channel closed during reload")),
                    }
                }
                finished = active_futs.next(), if !active_futs.is_empty() => {
                    if let Some((fin_id, res)) = finished {
                        active.remove(&fin_id);
                        if fin_id == id {
                            if let Err(e) = res {
                                return Err(format!("workflow '{id}' exited during reload: {e}"));
                            }
                            return Err(format!("workflow '{id}' exited before confirming reload"));
                        }
                        if let Err(e) = res {
                            return Err(format!("workflow '{fin_id}' failed during reload: {e}"));
                        }
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(format!("timed out waiting for workflow '{id}' to reload"));
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn apply_reload<'a>(
    kdl: Option<String>,
    config_path: Option<&std::path::Path>,
    current_config: &mut ServiceConfig,
    active: &mut HashMap<String, ActiveWorkflow>,
    active_futs: &mut FuturesUnordered<LocalBoxFuture<'a, (String, Result<(), SaciError>)>>,
    state_client: &Option<Arc<RedbStateClient>>,
    factory: &'a RefCell<WorkflowFactory>,
    lifecycle: &Option<Arc<LifecycleRegistry>>,
    dlq_registry: &Arc<DlqRegistry>,
    alerts: &AlertDispatcher,
    lineage: &Arc<crate::service::lineage::LineageDispatcher>,
    coord_root: &CancellationToken,
) -> Result<ReloadReport, String> {
    let raw_kdl = match kdl {
        Some(text) => text,
        None => {
            let path = config_path.ok_or_else(|| {
                "cannot reload from disk: service started without a config file path".to_string()
            })?;
            std::fs::read_to_string(path)
                .map_err(|e| format!("reading config file {}: {e}", path.display()))?
        }
    };

    let mut new_config = ServiceConfig::from_kdl(&raw_kdl).map_err(|e| e.to_string())?;
    if !matches!(new_config.mode, ServiceMode::Standalone { .. }) {
        return Err("cannot live-reload a standalone service into cluster mode".to_string());
    }
    crate::service::validate_build_capabilities(&new_config).map_err(|e| e.to_string())?;
    new_config.node.id = current_config.node.id;
    new_config.http.bind = current_config.http.bind.clone();

    let config_dir = config_path.and_then(|p| p.parent());
    let prev_by_id: HashMap<&str, &WorkflowSpec> = current_config
        .workflows
        .iter()
        .map(|w| (w.id.as_str(), w))
        .collect();
    let new_ids: HashSet<&str> = new_config.workflows.iter().map(|w| w.id.as_str()).collect();
    let global_changed = global_build_settings_differ(current_config, &new_config);

    let mut added_specs = Vec::new();
    let mut updated_specs = Vec::new();
    let mut unchanged_ids = Vec::new();

    for new_spec in &new_config.workflows {
        if !prev_by_id.contains_key(new_spec.id.as_str()) {
            added_specs.push(new_spec.clone());
            continue;
        }
        let mtimes = workflow_artifact_mtimes(new_spec, config_dir);
        let is_unchanged = if let Some(entry) = active.get(&new_spec.id) {
            !global_changed && specs_equal(&entry.spec, new_spec) && entry.artifact_mtimes == mtimes
        } else if let Some(prev_spec) = prev_by_id.get(new_spec.id.as_str()) {
            !global_changed && specs_equal(prev_spec, new_spec)
        } else {
            false
        };
        if is_unchanged {
            unchanged_ids.push(new_spec.id.clone());
        } else {
            updated_specs.push(new_spec.clone());
        }
    }

    let removed_ids: Vec<String> = current_config
        .workflows
        .iter()
        .filter(|w| !new_ids.contains(w.id.as_str()))
        .map(|w| w.id.clone())
        .collect();

    // Sync DLQ registry before building added/updated workflows so newly declared
    // DLQ queues attach to the shared HTTP registry rather than private instances.
    let snap = factory.borrow().snapshot_settings();
    factory.borrow_mut().update_config(&new_config);
    dlq_registry.sync_with_config(&new_config);

    let mut built_added: Vec<(WorkflowSpec, BuiltWorkflow, WorkflowTopology)> =
        Vec::with_capacity(added_specs.len());
    for spec in &added_specs {
        let build_res = factory.borrow_mut().build(spec);
        let built = match build_res {
            Ok(b) => b,
            Err(e) => {
                factory.borrow_mut().restore_settings(snap);
                dlq_registry.sync_with_config(current_config);
                return Err(format!("building added workflow '{}': {e}", spec.id));
            }
        };
        let wf_topo =
            workflow_topology(&new_config, factory.borrow().registry(), spec, &built.nodes);
        built_added.push((spec.clone(), built, wf_topo));
    }

    // Drain and remove deleted workflows cleanly before updating/spawning others
    // so freed channel slots or ports are available immediately.
    let mut to_drain = HashSet::new();
    for id in &removed_ids {
        if let Some(lc) = lifecycle {
            lc.remove_workflow(id);
        }
        if let Some(entry) = active.get(id) {
            entry.cancel.cancel();
            to_drain.insert(id.clone());
        }
    }
    if !to_drain.is_empty() {
        drain_workflow_ids(active_futs, active, to_drain)
            .await
            .map_err(|e| e.to_string())?;
    }

    // Apply updates: live-reload controlled active workflows, or stop+rebuild
    // if unsupervised / already exited. Track partial update errors without
    // leaving `current_config` / `active` out of sync.
    let mut reload_waiters = Vec::new();
    let mut respawn_specs = Vec::new();
    let mut stop_for_respawn = HashSet::new();
    let mut reload_errors = Vec::new();

    for spec in &updated_specs {
        if active.contains_key(&spec.id) {
            if let Some(lc) = lifecycle {
                match lc.send_reload_workflow(&spec.id, spec.clone()) {
                    Ok(rx) => reload_waiters.push((spec.id.clone(), rx)),
                    Err(e) => reload_errors.push(format!("sending reload to '{}': {e}", spec.id)),
                }
            } else if let Some(entry) = active.get(&spec.id) {
                entry.cancel.cancel();
                stop_for_respawn.insert(spec.id.clone());
                respawn_specs.push(spec.clone());
            }
        } else {
            if let Some(lc) = lifecycle {
                lc.remove_workflow(&spec.id);
            }
            respawn_specs.push(spec.clone());
        }
    }

    if !stop_for_respawn.is_empty() {
        drain_workflow_ids(active_futs, active, stop_for_respawn)
            .await
            .map_err(|e| e.to_string())?;
    }

    if !reload_waiters.is_empty()
        && let Err(e) = wait_reload_replies(active_futs, active, reload_waiters).await
    {
        reload_errors.push(e);
    }

    for spec in &updated_specs {
        if let Some(entry) = active.get_mut(&spec.id) {
            entry.spec = spec.clone();
            entry.artifact_mtimes = workflow_artifact_mtimes(spec, config_dir);
        }
    }

    let mut built_topos: HashMap<String, WorkflowTopology> = HashMap::new();
    for spec in respawn_specs {
        let build_res = factory.borrow_mut().build(&spec);
        let built = match build_res {
            Ok(b) => b,
            Err(e) => {
                reload_errors.push(format!("rebuilding workflow '{}': {e}", spec.id));
                continue;
            }
        };
        let wf_topo = workflow_topology(
            &new_config,
            factory.borrow().registry(),
            &spec,
            &built.nodes,
        );
        built_topos.insert(spec.id.clone(), wf_topo);
        let live_stats = lifecycle
            .as_ref()
            .map(|_| Arc::new(RwLock::new(WorkflowStats::default())));
        let channels = lifecycle
            .as_ref()
            .map(|lc| lc.register_dynamic(&spec, live_stats.clone()));
        let cancel_child = coord_root.child_token();
        let mtimes = workflow_artifact_mtimes(&spec, config_dir);
        active.insert(
            spec.id.clone(),
            ActiveWorkflow {
                spec: spec.clone(),
                artifact_mtimes: mtimes,
                cancel: cancel_child.clone(),
            },
        );
        active_futs.push(spawn_workflow(
            built,
            spec,
            cancel_child,
            live_stats,
            state_client.clone(),
            factory,
            channels,
        ));
    }

    for (spec, built, wf_topo) in built_added {
        built_topos.insert(spec.id.clone(), wf_topo);
        let live_stats = lifecycle
            .as_ref()
            .map(|_| Arc::new(RwLock::new(WorkflowStats::default())));
        let channels = lifecycle
            .as_ref()
            .map(|lc| lc.register_dynamic(&spec, live_stats.clone()));
        let cancel_child = coord_root.child_token();
        let mtimes = workflow_artifact_mtimes(&spec, config_dir);
        active.insert(
            spec.id.clone(),
            ActiveWorkflow {
                spec: spec.clone(),
                artifact_mtimes: mtimes,
                cancel: cancel_child.clone(),
            },
        );
        active_futs.push(spawn_workflow(
            built,
            spec,
            cancel_child,
            live_stats,
            state_client.clone(),
            factory,
            channels,
        ));
    }

    alerts.update_config(&new_config.alerts);
    lineage.update_config(&new_config, None);

    let topology_version = if let Some(inspector) = factory.borrow().inspector() {
        let existing = inspector.topology();
        let next = update_topology_from_config(
            &new_config,
            factory.borrow().registry(),
            &existing,
            &built_topos,
        );
        let version = next.version;
        inspector.set_topology(next);
        version
    } else {
        0
    };

    if let Some(lc) = lifecycle {
        lc.set_raw_kdl(raw_kdl.clone());
    }

    if let Some(client) = state_client {
        let name = config_path
            .and_then(|p| p.file_name())
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "saci.kdl".to_string());
        if let Err(e) = client.put_config(&name, raw_kdl.as_bytes()).await {
            tracing::warn!(error = %e, "failed to persist reloaded KDL config");
        }
    }

    *current_config = new_config;

    if !reload_errors.is_empty() {
        return Err(reload_errors.join("; "));
    }

    Ok(ReloadReport {
        added: added_specs.into_iter().map(|s| s.id).collect(),
        updated: updated_specs.into_iter().map(|s| s.id).collect(),
        removed: removed_ids,
        unchanged: unchanged_ids,
        topology_version,
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_standalone_with_reload<'a>(
    mut current_config: ServiceConfig,
    items: Vec<(BuiltWorkflow, CancellationToken, Option<SupervisorChannels>)>,
    runner_stats: Option<Vec<(String, Arc<RwLock<WorkflowStats>>)>>,
    state_client: Option<Arc<RedbStateClient>>,
    factory: &'a RefCell<WorkflowFactory>,
    lifecycle: Option<Arc<LifecycleRegistry>>,
    dlq_registry: Arc<DlqRegistry>,
    alerts: AlertDispatcher,
    lineage: Arc<crate::service::lineage::LineageDispatcher>,
    coord_root: CancellationToken,
    config_path: Option<PathBuf>,
    reload_rx: &mut tokio::sync::mpsc::UnboundedReceiver<ReloadEnvelope>,
) -> Result<(), SaciError> {
    let mut active: HashMap<String, ActiveWorkflow> = HashMap::with_capacity(items.len());
    let mut active_futs: FuturesUnordered<LocalBoxFuture<'a, (String, Result<(), SaciError>)>> =
        FuturesUnordered::new();

    let config_dir = config_path.as_deref().and_then(|p| p.parent());
    for (i, (built, cancel_child, channels)) in items.into_iter().enumerate() {
        let spec = current_config.workflows[i].clone();
        let live_stats = runner_stats.as_ref().and_then(|entries| {
            entries
                .iter()
                .find(|(id, _)| *id == spec.id)
                .map(|(_, lock)| Arc::clone(lock))
        });
        active.insert(
            spec.id.clone(),
            ActiveWorkflow {
                spec: spec.clone(),
                artifact_mtimes: workflow_artifact_mtimes(&spec, config_dir),
                cancel: cancel_child.clone(),
            },
        );
        active_futs.push(spawn_workflow(
            built,
            spec,
            cancel_child,
            live_stats,
            state_client.clone(),
            factory,
            channels,
        ));
    }

    #[cfg(unix)]
    let mut sighup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
        .map_err(|e| SaciError::generic(format!("failed to install SIGHUP handler: {e}")))?;
    #[cfg(not(unix))]
    let mut sighup = ();

    loop {
        if active_futs.is_empty() {
            return Ok(());
        }
        tokio::select! {
            item = active_futs.next() => {
                match item {
                    Some((id, Ok(()))) => {
                        active.remove(&id);
                    }
                    Some((id, Err(e))) => {
                        active.remove(&id);
                        for entry in active.values() {
                            entry.cancel.cancel();
                        }
                        return Err(e);
                    }
                    None => return Ok(()),
                }
            }
            Some(envelope) = reload_rx.recv() => {
                let res = apply_reload(
                    envelope.kdl,
                    config_path.as_deref(),
                    &mut current_config,
                    &mut active,
                    &mut active_futs,
                    &state_client,
                    factory,
                    &lifecycle,
                    &dlq_registry,
                    &alerts,
                    &lineage,
                    &coord_root,
                )
                .await;
                let _ = envelope.reply.send(res);
            }
            _ = next_sighup(&mut sighup) => {
                match apply_reload(
                    None,
                    config_path.as_deref(),
                    &mut current_config,
                    &mut active,
                    &mut active_futs,
                    &state_client,
                    factory,
                    &lifecycle,
                    &dlq_registry,
                    &alerts,
                    &lineage,
                    &coord_root,
                )
                .await {
                    Ok(report) => {
                        tracing::info!(
                            added = ?report.added,
                            updated = ?report.updated,
                            removed = ?report.removed,
                            unchanged = ?report.unchanged,
                            topology_version = report.topology_version,
                            "SIGHUP configuration reload applied"
                        );
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "SIGHUP configuration reload failed; keeping running workflows");
                    }
                }
            }
        }
    }
}
