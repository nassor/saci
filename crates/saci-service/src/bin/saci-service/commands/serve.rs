//! `saci-service serve`: start the SACI service.
//!
//! Loads config, initialises logging and OpenTelemetry, builds the service from
//! registered factories, wires the HTTP control plane and watchdog, then
//! dispatches to the standalone or cluster runner. Waits for SIGINT or SIGTERM
//! before draining all tasks within the 30-second shutdown budget.
//!
//! The `ready` flag flips as soon as the runner is spawned, not after the first
//! successful pipeline iteration. In cluster mode `cluster_probe` is `None`, so
//! `/status` reports `"cluster": null`.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use opentelemetry_prometheus::exporter;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use tokio::sync::RwLock;

use saci_service::SaciError;
use saci_service::service::RedbStateClient;
use saci_service::service::builder::rebuild_blocker;
use saci_service::service::config::{LogFormat, ServiceConfig, ServiceMode};
use saci_service::service::dlq::DlqRegistry;
use saci_service::service::factories::register_builtin_factories;
use saci_service::service::http::{ServiceModeLabel, ServiceState};
use saci_service::service::lifecycle::{
    LifecycleRegistryBuilder, SupervisorChannels, run_supervised,
};
use saci_service::service::standalone::StandaloneStats;
use saci_service::service::{ServiceBuilder, ShutdownCoordinator, serve_http, spawn_watchdog};

use crate::cli::{GlobalOpts, LogFormatArg, ServeArgs};

/// Total time a stop gets, shared by its two stages: each runner's own
/// cancellation drain (flush staged batches, `finish()` every sink, report
/// where flow control ended), then the HTTP and watchdog tasks. Overrunning it
/// exits 1 without printing the clean-stop line.
const SHUTDOWN_BUDGET: Duration = Duration::from_secs(30);

/// Entry point for the `serve` subcommand.
pub async fn run(global: &GlobalOpts, args: &ServeArgs) -> Result<(), SaciError> {
    let config_path = &global.config;
    let mut config = ServiceConfig::load(config_path)?;
    // Before logging, the store, the meter provider or the port: a config
    // naming a host this binary was not built with cannot run whatever else
    // succeeds, and refusing here keeps the message first rather than buried
    // under a startup banner.
    saci_service::service::validate_build_capabilities(&config)?;

    if let Some(node_id) = args.node_id {
        config.node.id = node_id;
    }
    if let Some(level) = &global.log_level {
        config.observability.log_level = level.clone();
    }
    if let Some(format) = &global.log_format {
        config.observability.log_format = match format {
            LogFormatArg::Pretty => LogFormat::Pretty,
            LogFormatArg::Json => LogFormat::Json,
        };
    }
    if let Some(endpoint) = &global.otlp_endpoint {
        config.observability.otlp_endpoint = Some(endpoint.clone());
    }
    if let Some(port) = args.port {
        // Replace only the port portion of the existing bind address.
        let existing = &config.http.bind;
        let host = existing
            .rsplit_once(':')
            .map(|(h, _)| h)
            .unwrap_or("127.0.0.1");
        config.http.bind = format!("{host}:{port}");
    }

    // Logging must be initialised before any tracing call. The inspector is
    // built here too, because its capture layer joins the same subscriber.
    let (telemetry, inspector) =
        saci_service::service::init_logging(&config.observability, config.node.id)?;
    tracing::info!(node_id = config.node.id, "saci-service starting");
    // With a `store` block configured, persist the raw config file (pre
    // env-substitution, so `${VAR}` secrets stay as references) before the
    // pipeline builds; an unreachable store fails startup here rather than
    // mid-run. `validate` and `cluster init` do not write.
    let state_client: Option<Arc<RedbStateClient>> = match &config.store {
        Some(saci_service::service::config::StoreConfig::Redb { path, .. }) => {
            let client = Arc::new(RedbStateClient::open(path)?);
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
    saci_service::metrics::init();

    // Forks of this binary register their own runtime and IO factories here:
    // register_builtin_factories(ServiceBuilder::new()).with_runtime(id, ...).
    // One shared half per declared queue, built before the workflows so the
    // runner and the control plane are two views of the same state.
    let dlq_registry = Arc::new(DlqRegistry::from_config(&config));
    let builder =
        register_builtin_factories(ServiceBuilder::new()).with_dlq_registry(dlq_registry.clone());
    // The builder publishes the topology into the inspector once it knows the
    // runtime and the source/sink sets.
    let builder = match inspector.clone() {
        Some(inspector) => builder.with_inspector(inspector),
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

    let coord = ShutdownCoordinator::new(Duration::from_secs(30));

    let liveness = Arc::new(AtomicU64::new(0));
    let ready = Arc::new(AtomicBool::new(false));
    let mode_label = match &config.mode {
        ServiceMode::Standalone { .. } => ServiceModeLabel::Standalone,
        ServiceMode::Cluster { .. } => ServiceModeLabel::Cluster,
    };
    let standalone_stats: Option<Vec<(String, Arc<RwLock<StandaloneStats>>)>> = match &config.mode {
        ServiceMode::Standalone { .. } => Some(
            built
                .iter()
                .map(|b| {
                    (
                        b.workflow_id.clone(),
                        Arc::new(RwLock::new(StandaloneStats::default())),
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
        // No cluster probe: in cluster mode /status reports "cluster": null.
        cluster_probe: None,
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
    println!("saci-service listening on {resolved_addr}");
    if state.inspector.as_ref().is_some_and(|i| i.ui_enabled()) {
        println!("dashboard at http://{resolved_addr}/ui");
    } else {
        println!("endpoints at http://{resolved_addr}/");
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

    // The runner runs inline rather than under tokio::spawn: BuiltService holds
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
                                    cfg,
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
                saci_service::service::run_cluster(b, &runner_config, cancel_child)
                    .await
                    .map(|_| ())
            }
            // `service`-only build: the config parses, so the refusal names
            // the feature instead of reporting `mode` as an unknown value.
            // `validate_build_capabilities` at the top of `run` already
            // returned this same error, so reaching here would mean the
            // config's mode changed underneath us; the arm exists to keep the
            // match exhaustive and answers identically either way.
            #[cfg(not(feature = "service-cluster"))]
            ServiceMode::Cluster { .. } => {
                // Drop the runner-only inputs explicitly so the async move
                // closure still captures them.
                drop(items);
                drop(runner_stats);
                drop(factory);
                Err(saci_service::service::factories::missing_cluster_host_error())
            }
        }
    };

    // `wait_for_signal` consumes `coord` and cancels the root token itself;
    // clone the token first so the runner-exits-first path can do the same.
    let shutdown_token = coord.root();

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
            // `wait_for_signal` already cancelled the root token, which every
            // runner polls, so this await is the drain and not a wait for new
            // work.
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
                    std::process::exit(1);
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
        std::process::exit(1);
    }
    // Flush spans while no task is still emitting, then print the last line.
    telemetry.shutdown().await;
    // `println!`, not `tracing::info!`: this line is the documented proof that
    // a stop was a drain, and the default `log_level="error"` would swallow an
    // event. It pairs with the startup banner, which is a `println!` for the
    // same reason.
    println!("saci-service stopped cleanly");

    // Cancellation from a clean shutdown signal is not an error.
    runner_result
}
