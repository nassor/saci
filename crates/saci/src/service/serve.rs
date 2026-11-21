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

use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use opentelemetry_prometheus::exporter;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::service::RedbStateClient;
use crate::service::builder::rebuild_blocker;
use crate::service::config::{ServiceConfig, ServiceMode};
use crate::service::dlq::DlqRegistry;
use crate::service::http::{ServiceModeLabel, ServiceState};
use crate::service::lifecycle::{LifecycleRegistryBuilder, SupervisorChannels, run_supervised};
use crate::service::standalone::WorkflowStats;
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

    let coord = ShutdownCoordinator::new(SHUTDOWN_BUDGET);

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

    // Lifecycle control is standalone-only: `ServiceConfig::validate` allows
    // exactly one workflow in cluster mode and stopping it is stopping the
    // node. With no registry the routes are never mounted.
    let control_enabled =
        matches!(config.mode, ServiceMode::Standalone { .. }) && config.http.control;
    let mut lifecycle_builder = LifecycleRegistryBuilder::new();
    let supervisor_channels: Vec<Option<SupervisorChannels>> = config
        .workflows
        .iter()
        .map(|workflow| {
            control_enabled.then(|| {
                lifecycle_builder.register(
                    &workflow.id,
                    workflow.name.as_deref(),
                    rebuild_blocker(workflow),
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
        lifecycle,
        dlq: (!dlq_registry.is_empty()).then_some(dlq_registry),
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
    // Every supervisor shares one factory: they are polled by the one
    // `join_all` below, on this task, so a shared borrow is enough and the
    // `borrow_mut` never spans an await.
    let factory = std::cell::RefCell::new(factory);
    let runner_config = config.clone();
    let runner_stats = standalone_stats.clone();

    let runner_fut = async move {
        match runner_config.mode {
            ServiceMode::Standalone { .. } => {
                let cfg = &runner_config;
                let factory = &factory;
                let runner_futs =
                    items
                        .into_iter()
                        .enumerate()
                        .map(|(i, (b, cancel_child, channels))| {
                            let stats = runner_stats.as_ref().and_then(|entries| {
                                entries
                                    .iter()
                                    .find(|(id, _)| *id == b.workflow_id)
                                    .map(|(_, lock)| lock.clone())
                            });
                            let state = state_client.clone();
                            async move {
                                run_supervised(
                                    b,
                                    &cfg.workflows[i],
                                    cancel_child,
                                    stats,
                                    state,
                                    factory,
                                    channels,
                                )
                                .await
                                .map(|_| ())
                            }
                        });
                let results = futures::future::join_all(runner_futs).await;
                results.into_iter().find(Result::is_err).unwrap_or(Ok(()))
            }
            #[cfg(feature = "service-cluster")]
            ServiceMode::Cluster { .. } => {
                let (b, cancel_child, _channels) = items
                    .into_iter()
                    .next()
                    .expect("cluster mode config validation guarantees exactly one workflow");
                crate::service::run_cluster(b, &runner_config, cancel_child)
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
