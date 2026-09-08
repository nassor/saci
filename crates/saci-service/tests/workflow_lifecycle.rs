//! Workflow lifecycle control, over a real socket.
//!
//! The fixture is a CSV `FileSource` linked straight to a CSV `FileSink` under
//! `run_mode kind="continuous"`: no wasm artifact, no Docker, no external
//! service, and a runner that keeps iterating so a pause is observable as
//! iterations that stop climbing.
//!
//! What is exercised end to end:
//!
//! - The pause gate itself: a parked runner completes no further iteration,
//!   and resuming releases it.
//! - `pause`/`resume`, `stop`/`start` and `restart` over
//!   `POST /api/workflows/{id}/{verb}`, each observed through
//!   `GET /api/workflows`.
//! - A stop that drains: the sink file is complete once the workflow reports
//!   `stopped`.
//! - The refusals: `404` for an unknown workflow, `409` for a verb that is
//!   illegal from the current state or for a workflow that cannot be rebuilt.
//! - `http { control #false }`: no `/api/workflows` at all, while `/health`
//!   still answers.
//!
//! ```text
//! cargo nextest run -p saci-service --all-features --test workflow_lifecycle
//! ```

#![cfg(all(
    feature = "service",
    feature = "connector-file",
    feature = "transformer-csv"
))]

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use tokio::task::LocalSet;
use tokio_util::sync::CancellationToken;

use saci_service::service::builder::{ServiceBuilder, rebuild_blocker};
use saci_service::service::config::ServiceConfig;
use saci_service::service::factories::register_builtin_factories;
use saci_service::service::http::{ServiceModeLabel, ServiceState, build_router};
use saci_service::service::lifecycle::{LifecycleRegistryBuilder, PauseHandle, RunControl};
use saci_service::service::standalone::StandaloneStats;
use saci_service::service::{run_standalone, run_supervised};

/// How long a state poll waits before it gives up and fails the test.
const SETTLE_BUDGET: Duration = Duration::from_secs(10);

/// A quoted KDL string reads backslashes as escapes, so a Windows path has to
/// go in with forward slashes.
fn path_text(path: &std::path::Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

/// One workflow, `orders`: a CSV file straight into a CSV file.
///
/// `run_mode` is `"continuous"` for a runner that keeps iterating, or
/// `"one_shot"` for one that reaches the end of its own work.
fn config_kdl(input: &str, output: &str, data_dir: &str, run_mode: &str, control: bool) -> String {
    let control = if control { "#true" } else { "#false" };
    format!(
        r#"
mode "standalone"

node id=1 name="saci-workflow-lifecycle" data_dir="{data_dir}"

run_mode kind="{run_mode}"

workflow "orders" name="Orders" {{
    transformer "csv_fmt" format="csv" {{
        options has_headers=#true
    }}

    source "orders_in" type="FileSource" component="Order" transformer="csv_fmt" {{
        config {{
            path "{input}"
            schema_fields "id" type="int64" nullable=#false
            schema_fields "total" type="float64" nullable=#false
        }}
    }}

    sink "orders_out" type="FileSink" component="Order" transformer="csv_fmt" {{
        config {{
            path "{output}"
            schema_fields "id" type="int64" nullable=#false
            schema_fields "total" type="float64" nullable=#false
        }}
    }}

    link from="orders_in" to="orders_out"
}}

http bind="127.0.0.1:0" control={control}

observability log_level="warn"
"#
    )
}

/// Two independent workflows, `orders` and `refunds`, each a CSV file into a
/// CSV file, both iterating forever. What the service-wide verbs act on.
fn two_workflow_config_kdl(input: &str, output: &str, data_dir: &str) -> String {
    format!(
        r#"
mode "standalone"

node id=1 name="saci-workflow-lifecycle-pair" data_dir="{data_dir}"

run_mode kind="continuous"

workflow "orders" {{
    transformer "orders_csv" format="csv" {{
        options has_headers=#true
    }}

    source "orders_in" type="FileSource" component="Order" transformer="orders_csv" {{
        config {{
            path "{input}"
            schema_fields "id" type="int64" nullable=#false
            schema_fields "total" type="float64" nullable=#false
        }}
    }}

    sink "orders_out" type="FileSink" component="Order" transformer="orders_csv" {{
        config {{
            path "{output}.orders.csv"
            schema_fields "id" type="int64" nullable=#false
            schema_fields "total" type="float64" nullable=#false
        }}
    }}

    link from="orders_in" to="orders_out"
}}

workflow "refunds" {{
    transformer "refunds_csv" format="csv" {{
        options has_headers=#true
    }}

    source "refunds_in" type="FileSource" component="Order" transformer="refunds_csv" {{
        config {{
            path "{input}"
            schema_fields "id" type="int64" nullable=#false
            schema_fields "total" type="float64" nullable=#false
        }}
    }}

    sink "refunds_out" type="FileSink" component="Order" transformer="refunds_csv" {{
        config {{
            path "{output}.refunds.csv"
            schema_fields "id" type="int64" nullable=#false
            schema_fields "total" type="float64" nullable=#false
        }}
    }}

    link from="refunds_in" to="refunds_out"
}}

http bind="127.0.0.1:0"

observability log_level="warn"
"#
    )
}

/// Two workflows joined by one in-process channel. `ServiceConfig::validate`
/// requires both halves, and each half is what makes its workflow
/// unrebuildable: the pair is created once per process.
///
/// `drain` blocks in `next_batch` for as long as the producer holds the
/// channel's only sender, which is exactly why the test drives `bridged`.
#[cfg(feature = "connector-channel")]
fn channel_config_kdl(input: &str, output: &str, data_dir: &str) -> String {
    format!(
        r#"
mode "standalone"

node id=1 name="saci-workflow-lifecycle-channel" data_dir="{data_dir}"

run_mode kind="continuous"

workflow "bridged" {{
    transformer "csv_fmt" format="csv" {{
        options has_headers=#true
    }}

    source "orders_in" type="FileSource" component="Order" transformer="csv_fmt" {{
        config {{
            path "{input}"
            schema_fields "id" type="int64" nullable=#false
            schema_fields "total" type="float64" nullable=#false
        }}
    }}

    sink "bridge_out" type="ChannelSink" component="Order" {{
        config name="lifecycle-bridge" {{
            schema_fields "id" type="int64" nullable=#false
            schema_fields "total" type="float64" nullable=#false
        }}
    }}

    link from="orders_in" to="bridge_out"
}}

workflow "drain" {{
    transformer "out_csv" format="csv" {{
        options has_headers=#true
    }}

    source "bridge_in" type="ChannelSource" component="Order" {{
        config name="lifecycle-bridge" {{
            schema_fields "id" type="int64" nullable=#false
            schema_fields "total" type="float64" nullable=#false
        }}
    }}

    sink "orders_out" type="FileSink" component="Order" transformer="out_csv" {{
        config {{
            path "{output}"
            schema_fields "id" type="int64" nullable=#false
            schema_fields "total" type="float64" nullable=#false
        }}
    }}

    link from="bridge_in" to="orders_out"
}}

http bind="127.0.0.1:0"

observability log_level="warn"
"#
    )
}

/// Write the fixture CSV plus a config, and return the config's path.
fn fixture(dir: &std::path::Path, kdl: impl FnOnce(&str, &str) -> String) -> std::path::PathBuf {
    let input = dir.join("orders.csv");
    std::fs::write(&input, "id,total\n1,10.5\n2,20.25\n3,30.75\n").expect("write the csv fixture");
    let config_path = dir.join("service.kdl");
    std::fs::write(&config_path, kdl(&path_text(&input), &path_text(dir))).expect("write config");
    config_path
}

/// A running service: supervisors on this `LocalSet`, the control plane on an
/// ephemeral port.
struct Harness {
    addr: String,
    cancel: CancellationToken,
    client: reqwest::Client,
}

impl Harness {
    /// Build every workflow, register it for control, serve the router, and
    /// start one supervisor per workflow.
    ///
    /// Must be called inside a [`LocalSet`]: a supervisor owns a
    /// `BuiltService`, so its future is not `Send`.
    async fn start(config_path: &std::path::Path) -> Self {
        saci_service::service::install_ring_provider();
        let config = ServiceConfig::load(config_path).expect("config loads");

        let mut factory = register_builtin_factories(ServiceBuilder::new()).into_factory(&config);
        let mut built = Vec::with_capacity(config.workflows.len());
        for workflow in &config.workflows {
            built.push(factory.build(workflow).expect("workflow builds"));
        }

        let mut registry_builder = LifecycleRegistryBuilder::new();
        let channels: Vec<_> = config
            .workflows
            .iter()
            .map(|workflow| {
                registry_builder.register(
                    &workflow.id,
                    workflow.name.as_deref(),
                    rebuild_blocker(workflow),
                )
            })
            .collect();
        let registry = Arc::new(registry_builder.build());

        let stats: Vec<(String, Arc<RwLock<StandaloneStats>>)> = built
            .iter()
            .map(|b| {
                (
                    b.workflow_id.clone(),
                    Arc::new(RwLock::new(StandaloneStats::default())),
                )
            })
            .collect();

        let state = ServiceState {
            node_id: config.node.id,
            node_name: config.node.name.clone(),
            mode: ServiceModeLabel::Standalone,
            started_at: Instant::now(),
            prometheus_registry: Arc::new(prometheus::Registry::new()),
            liveness: Arc::new(AtomicU64::new(0)),
            ready: Arc::new(AtomicBool::new(true)),
            cluster_probe: None,
            standalone_stats: Some(stats.clone()),
            inspector: None,
            lifecycle: config.http.control.then_some(registry),
            dlq: None,
        };

        let cancel = CancellationToken::new();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr").to_string();
        let router = build_router(state);
        let server_cancel = cancel.clone();
        tokio::spawn(async move {
            let _ = axum::serve(listener, router)
                .with_graceful_shutdown(async move { server_cancel.cancelled().await })
                .await;
        });

        let config = Rc::new(config);
        let factory = Rc::new(RefCell::new(factory));
        for (index, (service, control)) in built.into_iter().zip(channels).enumerate() {
            let config = config.clone();
            let factory = factory.clone();
            let live = stats[index].1.clone();
            let cancel = cancel.child_token();
            let control = config.http.control.then_some(control);
            tokio::task::spawn_local(async move {
                let _ = run_supervised(
                    service,
                    &config.workflows[index],
                    &config,
                    cancel,
                    Some(live),
                    None,
                    &factory,
                    control,
                )
                .await;
            });
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
        Self {
            addr,
            cancel,
            client: reqwest::Client::new(),
        }
    }

    async fn get(&self, path: &str) -> reqwest::Response {
        self.client
            .get(format!("http://{}{path}", self.addr))
            .send()
            .await
            .expect("GET reaches the control plane")
    }

    async fn post(&self, path: &str) -> reqwest::Response {
        self.client
            .post(format!("http://{}{path}", self.addr))
            .send()
            .await
            .expect("POST reaches the control plane")
    }

    /// One workflow's published status.
    async fn status(&self, id: &str) -> serde_json::Value {
        let response = self.get(&format!("/api/workflows/{id}")).await;
        assert_eq!(response.status(), 200, "GET /api/workflows/{id}");
        response.json().await.expect("status is JSON")
    }

    /// Poll until the workflow reports one of `wanted`, or fail.
    async fn await_state(&self, id: &str, wanted: &[&str]) -> serde_json::Value {
        let deadline = Instant::now() + SETTLE_BUDGET;
        let mut last = serde_json::Value::Null;
        while Instant::now() < deadline {
            last = self.status(id).await;
            let state = last["state"].as_str().unwrap_or_default();
            if wanted.contains(&state) {
                return last;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("workflow '{id}' never reached {wanted:?}; last status: {last}");
    }

    /// Poll until the workflow has moved at least `rows` rows, or fail.
    ///
    /// A `stop` that lands before the first pass has drained anything is a
    /// legitimate outcome with an empty sink file, so a test asserting on the
    /// drain waits for real work first.
    async fn await_rows(&self, rows: u64) {
        let deadline = Instant::now() + SETTLE_BUDGET;
        let mut last = serde_json::Value::Null;
        while Instant::now() < deadline {
            last = self
                .get("/status")
                .await
                .json()
                .await
                .expect("status is JSON");
            if last["standalone"][0]["rows_processed"]
                .as_u64()
                .is_some_and(|moved| moved >= rows)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("the workflow never moved {rows} rows; last status: {last}");
    }

    /// The first workflow's `iterations`, as `/status` reports it.
    async fn iterations(&self) -> u64 {
        let body: serde_json::Value = self
            .get("/status")
            .await
            .json()
            .await
            .expect("status is JSON");
        body["standalone"][0]["iterations"]
            .as_u64()
            .expect("a standalone workflow reports iterations")
    }

    fn stop(self) {
        self.cancel.cancel();
    }
}

#[tokio::test]
async fn pause_parks_the_runner_and_resume_releases_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("orders_out.csv");
    let out_text = path_text(&output);
    let config_path = fixture(dir.path(), |input, data_dir| {
        config_kdl(input, &out_text, data_dir, "continuous", true)
    });

    let config = ServiceConfig::load(&config_path).expect("config loads");
    let built = register_builtin_factories(ServiceBuilder::new())
        .build_all(&config)
        .expect("the file workflow builds")
        .remove(0);

    let live = Arc::new(RwLock::new(StandaloneStats::default()));
    let cancel = CancellationToken::new();
    let (handle, gate) = PauseHandle::new();
    let mut parked = handle.parked();

    let local = LocalSet::new();
    local
        .run_until(async {
            let runner = {
                let live = live.clone();
                let cancel = cancel.clone();
                tokio::task::spawn_local(async move {
                    run_standalone(
                        built,
                        &config,
                        RunControl::new(cancel, gate),
                        Some(live),
                        None,
                    )
                    .await
                })
            };

            // Let the runner get going, then park it.
            tokio::time::sleep(Duration::from_millis(200)).await;
            handle.request_pause();
            tokio::time::timeout(SETTLE_BUDGET, parked.changed())
                .await
                .expect("the runner parks within the budget")
                .expect("the pause handle outlives the runner");
            assert!(*parked.borrow_and_update(), "the runner reported parked");

            let frozen = live.read().await.iterations;
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert_eq!(
                live.read().await.iterations,
                frozen,
                "a parked runner completes no further iteration"
            );

            handle.request_resume();
            let deadline = Instant::now() + SETTLE_BUDGET;
            while Instant::now() < deadline && live.read().await.iterations == frozen {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            assert!(
                live.read().await.iterations > frozen,
                "resuming releases the runner"
            );

            cancel.cancel();
            let stats = tokio::time::timeout(SETTLE_BUDGET, runner)
                .await
                .expect("the runner exits within the budget")
                .expect("the runner task did not panic")
                .expect("cancellation is a clean exit");
            assert!(stats.iterations > frozen);
        })
        .await;
}

#[tokio::test]
async fn pause_and_resume_round_trip_over_http() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("orders_out.csv");
    let out_text = path_text(&output);
    let config_path = fixture(dir.path(), |input, data_dir| {
        config_kdl(input, &out_text, data_dir, "continuous", true)
    });

    LocalSet::new()
        .run_until(async {
            let harness = Harness::start(&config_path).await;
            harness.await_state("orders", &["running"]).await;

            let response = harness.post("/api/workflows/orders/pause").await;
            assert!(
                matches!(response.status().as_u16(), 200 | 202),
                "pause is accepted: {}",
                response.status()
            );
            let body: serde_json::Value = response.json().await.expect("pause answers a status");
            assert!(
                matches!(body["state"].as_str(), Some("pausing" | "paused")),
                "pause reports a transition: {body}"
            );

            let paused = harness.await_state("orders", &["paused"]).await;
            assert_eq!(paused["id"], "orders");
            assert_eq!(paused["name"], "Orders");
            assert_eq!(paused["restartable"], true);

            // The list endpoint agrees with the single-workflow one.
            let list: Vec<serde_json::Value> = harness
                .get("/api/workflows")
                .await
                .json()
                .await
                .expect("the list is JSON");
            assert_eq!(list.len(), 1, "one declared workflow");
            assert_eq!(list[0]["state"], "paused");

            // `/status` carries the same state, and the supervised runner
            // really stopped: asserting only the badge would pass on a
            // supervisor that publishes `paused` without parking anything.
            let status: serde_json::Value = harness
                .get("/status")
                .await
                .json()
                .await
                .expect("status is JSON");
            assert_eq!(status["standalone"][0]["state"], "paused");
            let frozen = harness.iterations().await;
            tokio::time::sleep(Duration::from_millis(400)).await;
            assert_eq!(
                harness.iterations().await,
                frozen,
                "a paused workflow completes no further iteration"
            );

            let response = harness.post("/api/workflows/orders/resume").await;
            assert!(matches!(response.status().as_u16(), 200 | 202));
            harness.await_state("orders", &["running"]).await;

            let deadline = Instant::now() + SETTLE_BUDGET;
            while Instant::now() < deadline && harness.iterations().await == frozen {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            assert!(
                harness.iterations().await > frozen,
                "resuming releases the runner"
            );

            harness.stop();
        })
        .await;
}

#[tokio::test]
async fn restart_starts_a_second_run() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("orders_out.csv");
    let out_text = path_text(&output);
    let config_path = fixture(dir.path(), |input, data_dir| {
        config_kdl(input, &out_text, data_dir, "continuous", true)
    });

    LocalSet::new()
        .run_until(async {
            let harness = Harness::start(&config_path).await;
            let first = harness.await_state("orders", &["running"]).await;
            assert_eq!(first["runs"], 1);

            let response = harness.post("/api/workflows/orders/restart").await;
            assert!(
                matches!(response.status().as_u16(), 200 | 202),
                "restart is accepted: {}",
                response.status()
            );

            let deadline = Instant::now() + SETTLE_BUDGET;
            loop {
                let status = harness.await_state("orders", &["running"]).await;
                if status["runs"] == 2 {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "restart never started a second runner: {status}"
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }

            harness.stop();
        })
        .await;
}

#[tokio::test]
async fn stop_then_start_drains_and_rebuilds() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("orders_out.csv");
    let out_text = path_text(&output);
    let config_path = fixture(dir.path(), |input, data_dir| {
        config_kdl(input, &out_text, data_dir, "continuous", true)
    });

    LocalSet::new()
        .run_until(async {
            let harness = Harness::start(&config_path).await;
            harness.await_state("orders", &["running"]).await;
            harness.await_rows(3).await;

            let response = harness.post("/api/workflows/orders/stop").await;
            assert!(
                matches!(response.status().as_u16(), 200 | 202),
                "stop is accepted: {}",
                response.status()
            );
            harness.await_state("orders", &["stopped"]).await;

            // The drain called `finish()`, so the sink file is complete.
            let written = std::fs::read_to_string(&output).expect("the sink wrote its file");
            assert!(
                written.contains("10.5") && written.contains("30.75"),
                "a stopped workflow leaves a complete sink file: {written:?}"
            );

            let response = harness.post("/api/workflows/orders/start").await;
            assert!(
                matches!(response.status().as_u16(), 200 | 202),
                "start is accepted: {}",
                response.status()
            );
            let running = harness.await_state("orders", &["running"]).await;
            assert_eq!(running["runs"], 2, "start built and ran a second time");

            harness.stop();
        })
        .await;
}

/// A `one_shot` workflow finishes its own work, and its supervisor returns so
/// the process can exit. Three behaviours hang off that, and this pins all
/// three: the published state is `completed`, the entry survives for reading,
/// and a rebuild verb answers `503` because nothing is listening any more.
/// The dashboard's own `verb_allowed` disables `start` and `restart` from
/// `completed` for the same reason.
#[tokio::test]
async fn a_completed_workflow_cannot_be_started_again() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("orders_out.csv");
    let out_text = path_text(&output);
    let config_path = fixture(dir.path(), |input, data_dir| {
        config_kdl(input, &out_text, data_dir, "one_shot", true)
    });

    LocalSet::new()
        .run_until(async {
            let harness = Harness::start(&config_path).await;
            let done = harness.await_state("orders", &["completed"]).await;
            assert_eq!(done["runs"], 1);
            assert_eq!(
                done["restartable"], true,
                "nothing about this workflow's shape blocks a rebuild"
            );

            for verb in ["start", "restart"] {
                let response = harness.post(&format!("/api/workflows/orders/{verb}")).await;
                assert_eq!(
                    response.status(),
                    503,
                    "{verb} on a completed workflow has no supervisor to reach"
                );
                let body: serde_json::Value = response.json().await.expect("a refusal is JSON");
                assert!(
                    body["error"]
                        .as_str()
                        .unwrap_or_default()
                        .contains("supervisor"),
                    "the refusal says why: {body}"
                );
            }

            harness.stop();
        })
        .await;
}

#[tokio::test]
async fn unknown_workflow_and_illegal_transition_are_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("orders_out.csv");
    let out_text = path_text(&output);
    let config_path = fixture(dir.path(), |input, data_dir| {
        config_kdl(input, &out_text, data_dir, "continuous", true)
    });

    LocalSet::new()
        .run_until(async {
            let harness = Harness::start(&config_path).await;
            harness.await_state("orders", &["running"]).await;

            let response = harness.post("/api/workflows/nope/stop").await;
            assert_eq!(response.status(), 404, "no such workflow");
            let body: serde_json::Value = response.json().await.expect("a refusal is JSON");
            assert!(
                body["error"].as_str().unwrap_or_default().contains("nope"),
                "the refusal names the workflow: {body}"
            );

            // Already running: resume is satisfied, not an error.
            let response = harness.post("/api/workflows/orders/resume").await;
            assert_eq!(response.status(), 200, "resume while running is a no-op");
            let body: serde_json::Value = response.json().await.expect("a status is JSON");
            assert_eq!(body["state"], "running");

            harness.post("/api/workflows/orders/stop").await;
            harness.await_state("orders", &["stopped"]).await;

            let response = harness.post("/api/workflows/orders/pause").await;
            assert_eq!(response.status(), 409, "pause from stopped is illegal");
            let body: serde_json::Value = response.json().await.expect("a refusal is JSON");
            assert!(
                body["error"].as_str().unwrap_or_default().contains("pause"),
                "the refusal names the verb: {body}"
            );

            harness.stop();
        })
        .await;
}

#[cfg(feature = "connector-channel")]
#[tokio::test]
async fn a_channel_workflow_refuses_the_rebuild_verbs_but_still_pauses() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("orders_out.csv");
    let out_text = path_text(&output);
    let config_path = fixture(dir.path(), |input, data_dir| {
        channel_config_kdl(input, &out_text, data_dir)
    });

    LocalSet::new()
        .run_until(async {
            let harness = Harness::start(&config_path).await;
            let status = harness.await_state("bridged", &["running"]).await;
            assert_eq!(status["restartable"], false);
            assert!(
                status["restart_blocked_reason"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("ChannelSink"),
                "the reason names the channel halves: {status}"
            );

            for verb in ["start", "stop", "restart"] {
                let response = harness
                    .post(&format!("/api/workflows/bridged/{verb}"))
                    .await;
                assert_eq!(
                    response.status(),
                    409,
                    "{verb} on a non-rebuildable workflow"
                );
            }

            // Pause keeps the runner and every resource it holds alive, so it
            // stays available.
            let response = harness.post("/api/workflows/bridged/pause").await;
            assert!(
                matches!(response.status().as_u16(), 200 | 202),
                "pause is accepted: {}",
                response.status()
            );
            harness.await_state("bridged", &["paused"]).await;

            harness.stop();
        })
        .await;
}

#[tokio::test]
async fn control_disabled_has_no_workflow_routes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("orders_out.csv");
    let out_text = path_text(&output);
    let config_path = fixture(dir.path(), |input, data_dir| {
        config_kdl(input, &out_text, data_dir, "continuous", false)
    });

    LocalSet::new()
        .run_until(async {
            let harness = Harness::start(&config_path).await;

            for path in [
                "/api/workflows",
                "/api/workflows/orders",
                "/api/workflows/orders/pause",
            ] {
                let response = harness.get(path).await;
                assert_eq!(response.status(), 404, "{path} is not mounted");
            }

            let response = harness.get("/health").await;
            assert_eq!(response.status(), 200, "the control plane still answers");

            harness.stop();
        })
        .await;
}

#[tokio::test]
async fn a_service_wide_verb_moves_every_workflow() {
    let dir = tempfile::tempdir().expect("tempdir");
    let prefix = dir.path().join("out");
    let prefix_text = path_text(&prefix);
    let config_path = fixture(dir.path(), |input, data_dir| {
        two_workflow_config_kdl(input, &prefix_text, data_dir)
    });

    LocalSet::new()
        .run_until(async {
            let harness = Harness::start(&config_path).await;
            harness.await_state("orders", &["running"]).await;
            harness.await_state("refunds", &["running"]).await;

            let response = harness.post("/api/service/pause").await;
            assert!(
                matches!(response.status().as_u16(), 200 | 202),
                "a service-wide pause is accepted: {}",
                response.status()
            );
            let report: serde_json::Value = response.json().await.expect("a report is JSON");
            assert_eq!(
                report["applied"].as_array().map(Vec::len),
                Some(2),
                "both workflows took the verb: {report}"
            );
            assert_eq!(
                report["refused"].as_array().map(Vec::len),
                Some(0),
                "nothing refused it: {report}"
            );
            harness.await_state("orders", &["paused"]).await;
            harness.await_state("refunds", &["paused"]).await;

            let response = harness.post("/api/service/resume").await;
            assert!(matches!(response.status().as_u16(), 200 | 202));
            harness.await_state("orders", &["running"]).await;
            harness.await_state("refunds", &["running"]).await;

            let response = harness.post("/api/service/stop").await;
            assert!(matches!(response.status().as_u16(), 200 | 202));
            harness.await_state("orders", &["stopped"]).await;
            harness.await_state("refunds", &["stopped"]).await;

            let response = harness.post("/api/service/start").await;
            assert!(matches!(response.status().as_u16(), 200 | 202));
            for id in ["orders", "refunds"] {
                let status = harness.await_state(id, &["running"]).await;
                assert_eq!(status["runs"], 2, "{id} was built and run a second time");
            }

            harness.stop();
        })
        .await;
}

/// A verb no workflow can take is the one case that fails the request. Both
/// workflows here are bound to one in-process channel, so neither can be torn
/// down and built again.
#[cfg(feature = "connector-channel")]
#[tokio::test]
async fn a_service_wide_verb_reports_the_workflows_it_cannot_reach() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("orders_out.csv");
    let out_text = path_text(&output);
    let config_path = fixture(dir.path(), |input, data_dir| {
        channel_config_kdl(input, &out_text, data_dir)
    });

    LocalSet::new()
        .run_until(async {
            let harness = Harness::start(&config_path).await;
            harness.await_state("bridged", &["running"]).await;

            let response = harness.post("/api/service/stop").await;
            assert_eq!(
                response.status(),
                409,
                "not one workflow could take the verb"
            );
            let report: serde_json::Value = response.json().await.expect("a report is JSON");
            assert_eq!(report["applied"].as_array().map(Vec::len), Some(0));
            assert_eq!(
                report["refused"].as_array().map(Vec::len),
                Some(2),
                "both halves of the channel are named: {report}"
            );
            assert!(
                report["refused"][0]["error"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("ChannelSink"),
                "the refusal carries the reason: {report}"
            );

            harness.stop();
        })
        .await;
}
