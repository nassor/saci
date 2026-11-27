//! Integration tests for `saci-service`. Each test spawns the `saci-service` binary
//! as a subprocess and drives it over HTTP or by its exit code.
//!
//! `cargo build --features service` must run first so the binary exists at
//! `env!("CARGO_BIN_EXE_saci-service")`.
//!
//! ```text
//! cargo test --test service_integration --all-features -- --test-threads=4
//! ```
//!
//! Tests pass `--bind 127.0.0.1:0` and parse the `saci-service listening on <addr>` line from
//! stdout, so no port is hardcoded. Each test gets its own
//! [`tempfile::TempDir`] for `node.data_dir`, and [`Service`] kills and reaps
//! the child even if the test panics.

#![cfg(feature = "service")]

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tempfile::NamedTempFile;

// Path to the compiled binary, set by cargo for integration tests.
const BIN: &str = env!("CARGO_BIN_EXE_saci-service");

/// How long a freshly spawned service gets to bind and answer.
///
/// The `Test` job runs the whole workspace suite, over 1700 tests, on a shared
/// two core runner, so a `serve` child competes for those two cores with a
/// Kafka container, the Raft chaos clusters and every other test in flight.
/// Startup itself costs about 0.3 s on an idle machine. 60 s of headroom over
/// that costs a healthy run nothing: every wait below returns as soon as the
/// endpoint answers, and returns early, with the child's exit status and its
/// stderr, as soon as the child dies. A broken binary therefore fails in
/// milliseconds instead of waiting the budget out.
const STARTUP_BUDGET: Duration = Duration::from_secs(60);

/// How long the service gets to exit after being asked to.
///
/// Shutdown drains a continuous runner and joins the HTTP server under the same
/// oversubscription that startup faces.
const SHUTDOWN_BUDGET: Duration = Duration::from_secs(30);

/// A spawned `saci-service serve` child, its bound address, and everything a
/// failing wait needs to explain itself.
///
/// A reader thread drains the child's stdout to EOF for the child's whole
/// lifetime. That is load bearing rather than tidy: `serve` prints the bind
/// address, then a second banner line, and only then spawns the HTTP server. A
/// parent that stops reading at the address line drops the read end of the
/// pipe, the child's next `println!` fails with `BrokenPipe`, and `println!`
/// panics on a write error, so the process dies between binding the port and
/// serving it. The endpoint then never answers and the port is nobody's, which
/// looks exactly like a slow start.
///
/// Stderr goes to a file rather than to a pipe: nothing has to read it while
/// the service runs, and every panic below can quote it.
struct Service {
    child: std::process::Child,
    /// The address `serve` reported, `host:port`.
    addr: String,
    /// Every line the child has written to stdout, in order.
    stdout: Arc<Mutex<Vec<String>>>,
    stderr: NamedTempFile,
}

impl Drop for Service {
    fn drop(&mut self) {
        // Best-effort kill; ignore errors (process may have already exited).
        self.child.kill().ok();
        self.child.wait().ok();
    }
}

impl Service {
    /// Spawn `serve --config <path> --bind 127.0.0.1:0` and wait for its bind address.
    ///
    /// # Panics
    ///
    /// Panics if the child exits first, or if the line does not arrive within
    /// [`STARTUP_BUDGET`]. Both messages carry the child's stdout and stderr.
    fn spawn(config_path: &Path) -> Self {
        let stderr = NamedTempFile::new().expect("tempfile for the child's stderr");
        let stderr_sink = stderr.reopen().expect("reopen the child's stderr file");
        let mut child = std::process::Command::new(BIN)
            .arg("serve")
            .arg("--config")
            .arg(config_path)
            .arg("--bind")
            .arg("127.0.0.1:0")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::from(stderr_sink))
            .spawn()
            .expect("failed to spawn saci-service");

        let pipe = child.stdout.take().expect("stdout piped");
        let stdout = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&stdout);
        std::thread::spawn(move || {
            // To EOF, whatever the parent is doing with the lines: the child
            // keeps printing after the address, and a closed pipe kills it.
            for line in BufReader::new(pipe).lines() {
                match line {
                    Ok(line) => lock(&sink).push(line),
                    Err(_) => return,
                }
            }
        });

        let deadline = Instant::now() + STARTUP_BUDGET;
        let addr = loop {
            let found = lock(&stdout).iter().find_map(|line| {
                line.strip_prefix("saci-service listening on ")
                    .map(|rest| rest.trim().to_string())
            });
            if let Some(addr) = found {
                break addr;
            }
            if let Some(status) = child.try_wait().expect("try_wait on the service child") {
                panic!(
                    "the service exited with {status} before printing its bind address{}",
                    diagnostics(&stdout, &stderr)
                );
            }
            if Instant::now() >= deadline {
                panic!(
                    "timed out after {STARTUP_BUDGET:?} waiting for the \
                     'saci-service listening on' line{}",
                    diagnostics(&stdout, &stderr)
                );
            }
            std::thread::sleep(Duration::from_millis(25));
        };

        Self {
            child,
            addr,
            stdout,
            stderr,
        }
    }

    /// `http://<addr><path>`, for example `/health`.
    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    /// The child's stdout and stderr, for a failure message.
    fn diagnostics(&self) -> String {
        diagnostics(&self.stdout, &self.stderr)
    }

    /// Why a wait on a live service cannot succeed, once the child has exited.
    ///
    /// [`std::process::Child::try_wait`] caches the status, so asking again
    /// after the child is reaped still answers.
    fn died(&mut self) -> Option<String> {
        let status = self
            .child
            .try_wait()
            .expect("try_wait on the service child")?;
        Some(format!("it exited with {status}{}", self.diagnostics()))
    }

    /// Poll `url` with GET until a 200 response, the child exits, or `timeout`
    /// elapses.
    async fn poll_until_200(&mut self, url: &str, timeout: Duration) -> Result<(), String> {
        let client = probe_client();
        let deadline = Instant::now() + timeout;
        loop {
            if let Ok(resp) = client.get(url).send().await
                && resp.status().is_success()
            {
                return Ok(());
            }
            if let Some(reason) = self.died() {
                return Err(format!("the service never answered {url}: {reason}"));
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "timed out polling {url} after {timeout:?}{}",
                    self.diagnostics()
                ));
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    }

    /// Poll `url` with GET until the body contains every string in `needles`,
    /// the child exits, or `timeout` elapses. Returns the matching body.
    async fn poll_until_body_contains(
        &mut self,
        url: &str,
        needles: &[&str],
        timeout: Duration,
    ) -> Result<String, String> {
        let client = probe_client();
        let deadline = Instant::now() + timeout;
        let mut last = String::new();
        loop {
            if let Ok(resp) = client.get(url).send().await
                && resp.status().is_success()
                && let Ok(body) = resp.text().await
            {
                if needles.iter().all(|n| body.contains(n)) {
                    return Ok(body);
                }
                last = body;
            }
            if let Some(reason) = self.died() {
                return Err(format!(
                    "the service stopped answering {url} before its body carried \
                     {needles:?}: {reason}\nlast body:\n{last}"
                ));
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "timed out polling {url} after {timeout:?}{}\nlast body:\n{last}",
                    self.diagnostics()
                ));
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    }

    /// Ask the service to shut down: SIGTERM on unix, a hard kill elsewhere.
    fn terminate(&self) {
        #[cfg(unix)]
        unsafe {
            libc::kill(self.child.id() as libc::pid_t, libc::SIGTERM);
        }
        #[cfg(not(unix))]
        {
            // Windows: no SIGTERM, kill directly via a separate Command. Its
            // own output is discarded, so a child that already exited does not
            // leave a taskkill error in the test's stderr.
            std::process::Command::new("taskkill")
                .args(["/PID", &self.child.id().to_string(), "/F"])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .ok();
        }
    }

    /// Wait for the child to exit, polling every 100 ms. `None` on timeout.
    fn wait_exit(&mut self, timeout: Duration) -> Option<std::process::ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            match self.child.try_wait() {
                Ok(Some(status)) => return Some(status),
                Ok(None) => {}
                Err(_) => return None,
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

/// A client whose per-request timeout is short enough that a stuck socket does
/// not eat the whole budget in one attempt.
fn probe_client() -> reqwest::Client {
    saci::service::install_ring_provider();
    reqwest::Client::builder()
        .timeout(Duration::from_millis(500))
        .build()
        .expect("build the probe client")
}

/// Lock through a poisoned mutex: a panicking reader thread must not make the
/// child's output unreadable, which is exactly when it is wanted.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Both of the child's output streams, appended to a failure message.
fn diagnostics(stdout: &Mutex<Vec<String>>, stderr: &NamedTempFile) -> String {
    let out = lock(stdout).join("\n");
    let err = std::fs::read_to_string(stderr.path()).unwrap_or_default();
    format!("\n--- child stdout ---\n{out}\n--- child stderr ---\n{err}")
}

/// Writes a minimal standalone config to a temp file. `data_dir` keeps each
/// test isolated, and `http.bind` uses port 0 so the binary picks an ephemeral
/// port and prints the address.
fn write_standalone_config(data_dir: &std::path::Path) -> NamedTempFile {
    let mut f = NamedTempFile::new().expect("tempfile");
    let data_dir_str = data_dir.to_string_lossy().replace('\\', "/");

    // FileSource/FileSink give a real, always-buildable pipeline: the file
    // exists (or, for the sink, its parent directory does) before the
    // service ever boots, matching examples/configs/standalone.kdl. A
    // ChannelSource/ChannelSink pair would need a `name` and a registered
    // channel bridge, which is beside the point for a boot/HTTP smoke test.
    let input_path = data_dir.join("smoke-in.csv");
    std::fs::write(&input_path, "id\n1\n").expect("write input csv");
    let input_path_str = input_path.to_string_lossy().replace('\\', "/");
    let output_path_str = data_dir
        .join("smoke-out.csv")
        .to_string_lossy()
        .replace('\\', "/");

    write!(
        f,
        r#"
mode "standalone"

node id=1 data_dir="{data_dir_str}"

run "continuous"

workflow "smoke" {{
    codec "csv_fmt" format="csv" {{
        config has_headers=#true
    }}
    source "in" connector="file" component="Ping" codec="csv_fmt" {{
        config {{
            path "{input_path_str}"
            field "id" type="int64" nullable=#false
        }}
    }}
    sink "out" connector="file" component="Ping" codec="csv_fmt" {{
        config {{
            path "{output_path_str}"
            field "id" type="int64" nullable=#false
        }}
    }}
    link from="in" to="out"
}}

http bind="127.0.0.1:0"
"#
    )
    .expect("write config");
    f
}

/// Write a deliberately malformed config to a temp file.
fn write_bad_config() -> NamedTempFile {
    let mut f = NamedTempFile::new().expect("tempfile");
    // Unterminated string, so the KDL parse fails.
    writeln!(f, "this \"unclosed").expect("write bad config");
    f
}

#[test]
fn test_help_output_contains_all_subcommands() {
    let output = std::process::Command::new(BIN)
        .arg("--help")
        .output()
        .expect("failed to run saci-service --help");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{stdout}{stderr}");

    for cmd in &["serve", "validate", "status", "cluster"] {
        assert!(
            combined.contains(cmd),
            "help output missing '{cmd}': {combined}"
        );
    }
}

#[test]
fn test_validate_valid_config_exits_zero() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let config = write_standalone_config(dir.path());
    let status = std::process::Command::new(BIN)
        .arg("validate")
        .arg("--config")
        .arg(config.path())
        .status()
        .expect("failed to run validate");

    assert!(status.success(), "validate should exit 0 on valid config");
}

#[test]
fn test_validate_invalid_config_exits_nonzero() {
    let config = write_bad_config();
    let status = std::process::Command::new(BIN)
        .arg("validate")
        .arg("--config")
        .arg(config.path())
        .status()
        .expect("failed to run validate");

    assert!(
        !status.success(),
        "validate should exit nonzero on invalid config"
    );
}

#[test]
fn test_validate_output_contains_node_info() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let config = write_standalone_config(dir.path());
    let output = std::process::Command::new(BIN)
        .arg("validate")
        .arg("--config")
        .arg(config.path())
        .output()
        .expect("failed to run validate");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("node.id"),
        "validate output should contain node.id: {stdout}"
    );
    assert!(
        stdout.contains("standalone"),
        "validate output should contain mode: {stdout}"
    );
}

#[tokio::test]
async fn test_serve_health_endpoint_returns_200() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let config = write_standalone_config(dir.path());

    let mut service = Service::spawn(config.path());
    let health_url = service.url("/health");
    let polled = service.poll_until_200(&health_url, STARTUP_BUDGET).await;

    service.terminate();
    let exit = service.wait_exit(SHUTDOWN_BUDGET);

    polled.unwrap_or_else(|e| panic!("/health must answer 200 after startup: {e}"));
    assert!(
        exit.is_some(),
        "the service must exit within {SHUTDOWN_BUDGET:?} of SIGTERM{}",
        service.diagnostics()
    );
}

/// `/metrics` must carry series that real code writes, not just descriptors.
///
/// `saci_workflow_passes_total` comes from the standalone runner's pass
/// counter and the three service gauges come from the HTTP watchdog tick, so a
/// body containing all four proves both writers ran. The pass counter is
/// recorded once per workflow, so every sample of it names its `workflow`.
#[tokio::test]
async fn test_metrics_endpoint_exposes_written_series() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let config = write_standalone_config(dir.path());

    let mut service = Service::spawn(config.path());
    let metrics_url = service.url("/metrics");
    let expected = [
        "saci_workflow_passes_total{",
        "saci_watchdog_ticks_total",
        "saci_ready",
        "saci_uptime_seconds",
    ];
    let body = service
        .poll_until_body_contains(&metrics_url, &expected, STARTUP_BUDGET)
        .await;

    service.terminate();
    let exit = service.wait_exit(SHUTDOWN_BUDGET);

    let body = body.unwrap_or_else(|e| panic!("{e}"));
    for name in expected {
        assert!(
            body.contains(name),
            "/metrics should carry {name}, body was:\n{body}"
        );
    }
    let passes: Vec<&str> = body
        .lines()
        .filter(|line| line.starts_with("saci_workflow_passes_total{"))
        .collect();
    assert!(
        !passes.is_empty(),
        "/metrics should carry a saci_workflow_passes_total sample, body was:\n{body}"
    );
    for line in &passes {
        assert!(
            line.contains("workflow=\""),
            "every saci_workflow_passes_total sample names its workflow: {line}"
        );
    }
    assert!(
        exit.is_some(),
        "the service must exit within {SHUTDOWN_BUDGET:?} of SIGTERM{}",
        service.diagnostics()
    );
}

#[tokio::test]
async fn test_status_subcommand_hits_running_service() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let config = write_standalone_config(dir.path());

    let mut service = Service::spawn(config.path());
    let health_url = service.url("/health");
    service
        .poll_until_200(&health_url, STARTUP_BUDGET)
        .await
        .unwrap_or_else(|e| panic!("the service did not start: {e}"));

    let addr = service.url("");
    let output = std::process::Command::new(BIN)
        .arg("status")
        .arg("--url")
        .arg(&addr)
        .output()
        .expect("failed to run status");

    let status_stdout = String::from_utf8_lossy(&output.stdout);
    let status_stderr = String::from_utf8_lossy(&output.stderr);

    service.terminate();
    service.wait_exit(SHUTDOWN_BUDGET);

    assert!(
        output.status.success(),
        "status command should exit 0, stderr: {status_stderr}"
    );
    assert!(
        status_stdout.contains("node"),
        "status output should contain 'node': {status_stdout}"
    );
}

/// `serve` is callable as a library: cancelling `ServeOptions::shutdown` drains
/// the runner, the HTTP server and the watchdog inside the budget and reports
/// `Clean`, with `announce: false` keeping stdout quiet.
///
/// This is the only test in the binary that calls `serve` in-process, so the
/// process-wide tracing subscriber and meter provider it installs are its own.
#[tokio::test]
async fn test_serve_returns_clean_after_shutdown_token_fires() {
    use saci::service::{ServeExit, ServeOptions, ServiceBuilder, ServiceConfig, serve};

    let dir = tempfile::TempDir::new().expect("tempdir");
    let config_file = write_standalone_config(dir.path());
    let text = std::fs::read_to_string(config_file.path()).expect("read config");
    let config = ServiceConfig::from_kdl(&text).expect("config parses");

    let shutdown = tokio_util::sync::CancellationToken::new();
    let options = ServeOptions {
        config_path: None,
        announce: false,
        shutdown: Some(shutdown.clone()),
    };
    // `serve` runs its runners inline and is not `Send`, so it shares this
    // task with the trigger instead of being spawned. `fired` records that
    // the token was cancelled before `serve` returned: a runner that ended on
    // its own would also yield `Clean`, and this tells the two apart.
    let fired = std::sync::atomic::AtomicBool::new(false);
    let stop = async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        fired.store(true, std::sync::atomic::Ordering::SeqCst);
        shutdown.cancel();
    };
    let run = async {
        let exit = serve(config, ServiceBuilder::new(), options).await;
        (exit, fired.load(std::sync::atomic::Ordering::SeqCst))
    };
    let ((exit, stopped_by_token), ()) =
        tokio::time::timeout(STARTUP_BUDGET + SHUTDOWN_BUDGET, async {
            tokio::join!(run, stop)
        })
        .await
        .expect("serve must stop within its budget");

    assert!(
        stopped_by_token,
        "serve returned before the shutdown token was cancelled"
    );
    assert_eq!(
        exit.expect("serve returns Ok after a requested shutdown"),
        ServeExit::Clean
    );
}

#[cfg(all(
    feature = "connector-file",
    feature = "codec-csv",
    feature = "connector-redb"
))]
#[tokio::test]
async fn test_backfill_and_cursor_endpoints_and_cli() {
    use saci::service::{RedbStateClient, SourceCursorMeta};

    let dir = tempfile::TempDir::new().expect("tempdir");
    let d = dir.path().to_string_lossy().replace('\\', "/");
    let input_csv = dir.path().join("events.csv");
    std::fs::write(
        &input_csv,
        "id,ts\n1,2025-01-15T00:00:00Z\n2,2025-02-05T12:00:00Z\n3,2025-03-01T00:00:00Z\n",
    )
    .expect("write input csv");

    let redb_path = dir.path().join("state.redb");
    {
        let state = RedbStateClient::open(&redb_path).expect("open redb");
        state
            .save_source_cursor(
                "orders",
                "src_in",
                SourceCursorMeta {
                    items_processed: 10,
                    last_batch_at_ms: 1_000,
                },
            )
            .await
            .expect("save cursor 1");
        tokio::time::sleep(Duration::from_millis(10)).await;
        let t_mid = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        tokio::time::sleep(Duration::from_millis(10)).await;
        state
            .save_source_cursor(
                "orders",
                "src_in",
                SourceCursorMeta {
                    items_processed: 25,
                    last_batch_at_ms: 2_000,
                },
            )
            .await
            .expect("save cursor 2");
        assert!(t_mid > 0);
    }

    let kdl = format!(
        r#"
mode "standalone"
node id=1 name="backfill-node" data_dir="{d}"
run "continuous"

variables {{
    OUT_TAG "live"
}}

store "redb" path="{d}/state.redb" batch_resume=#true

workflow "orders" {{
    codec "csv_fmt" format="csv" {{
        config has_headers=#true
    }}
    source "src_in" connector="file" component="Event" codec="csv_fmt" {{
        config {{
            path "{d}/events.csv"
            field "id" type="int64" nullable=#false
            field "ts" type="utf8" nullable=#false
        }}
    }}
    sink "dst_out" connector="file" component="Event" codec="csv_fmt" {{
        config {{
            path "{d}/events_${{OUT_TAG}}.csv"
            truncate #true
            field "id" type="int64" nullable=#false
            field "ts" type="utf8" nullable=#false
        }}
    }}
    link from="src_in" to="dst_out"
}}

http bind="127.0.0.1:0" control=#true
observability log_level="warn"
"#
    );
    let cfg_path = dir.path().join("service.kdl");
    std::fs::write(&cfg_path, &kdl).expect("write config");

    let mut service = Service::spawn(&cfg_path);
    let health_url = service.url("/health");
    service
        .poll_until_200(&health_url, STARTUP_BUDGET)
        .await
        .expect("service starts");

    let client = probe_client();
    // 1. Trigger parameterized time-windowed backfill over HTTP.
    let bf_resp = client
        .post(service.url("/api/workflows/orders/backfill"))
        .json(&serde_json::json!({
            "start_date": "2025-02-01T00:00:00Z",
            "end_date": "2025-02-10T23:59:59Z",
            "time_field": "ts",
            "params": { "OUT_TAG": "bf1" },
            "reset_state": true
        }))
        .send()
        .await
        .expect("POST backfill");
    assert_eq!(bf_resp.status(), 200);
    let bf_json: serde_json::Value = bf_resp.json().await.expect("backfill JSON");
    assert_eq!(bf_json["workflow"], "orders");
    assert_eq!(bf_json["rows_processed"], 1);
    let bf_out = std::fs::read_to_string(dir.path().join("events_bf1.csv")).expect("bf1 output");
    assert!(bf_out.contains("2,2025-02-05T12:00:00Z"), "got: {bf_out}");
    assert!(!bf_out.contains("1,2025-01-15"), "outside window: {bf_out}");

    // 2. Inspect cursors and roll back to earliest checkpoint via HTTP + CLI.
    let cursors_resp = client
        .get(service.url("/api/workflows/orders/cursors"))
        .send()
        .await
        .expect("GET cursors");
    assert_eq!(cursors_resp.status(), 200);
    let cursors_json: serde_json::Value = cursors_resp.json().await.expect("cursors JSON");
    let checkpoints = cursors_json["checkpoints_unix_ms"]
        .as_array()
        .expect("checkpoints array");
    assert!(
        checkpoints.len() >= 2,
        "expected >=2 checkpoints: {cursors_json}"
    );
    let first_ts = checkpoints[0].as_u64().expect("u64 timestamp");

    let reset_resp = client
        .post(service.url("/api/workflows/orders/cursors/reset"))
        .json(&serde_json::json!({ "to_unix_ms": first_ts }))
        .send()
        .await
        .expect("POST cursors/reset");
    assert_eq!(reset_resp.status(), 200);
    let reset_json: serde_json::Value = reset_resp.json().await.expect("reset JSON");
    assert_eq!(reset_json["cursors"][0]["items_processed"], 10);

    // Also verify `saci-service cursor list` CLI subcommand against the live server.
    let cli_out = std::process::Command::new(BIN)
        .args(["cursor", "list", "orders", "--url", &service.url("")])
        .output()
        .expect("run cursor list CLI");
    assert!(
        cli_out.status.success(),
        "cursor list failed: {}",
        String::from_utf8_lossy(&cli_out.stderr)
    );

    service.terminate();
    service.wait_exit(SHUTDOWN_BUDGET);
}

#[cfg(all(
    feature = "connector-file",
    feature = "codec-csv",
    feature = "inspector"
))]
#[tokio::test]
async fn test_live_reload_over_http_and_sighup() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let d = dir.path().to_string_lossy().replace('\\', "/");
    let in_csv = dir.path().join("orders.csv");
    std::fs::write(&in_csv, "id,total\n1,10.0\n").expect("write csv");

    let kdl_v1 = format!(
        r#"
mode "standalone"
node id=1 name="reload-node" data_dir="{d}"
run "continuous"

workflow "steady" {{
    codec "steady_csv" format="csv" {{ config has_headers=#true }}
    source "steady_in" connector="file" component="Order" codec="steady_csv" {{
        config {{
            path "{d}/orders.csv"
            field "id" type="int64" nullable=#false
            field "total" type="float64" nullable=#false
        }}
    }}
    sink "steady_out" connector="file" component="Order" codec="steady_csv" {{
        config {{
            path "{d}/steady_out.csv"
            truncate #true
            field "id" type="int64" nullable=#false
            field "total" type="float64" nullable=#false
        }}
    }}
    link from="steady_in" to="steady_out"
}}

workflow "legacy" {{
    codec "legacy_csv" format="csv" {{ config has_headers=#true }}
    source "legacy_in" connector="file" component="Order" codec="legacy_csv" {{
        config {{
            path "{d}/orders.csv"
            field "id" type="int64" nullable=#false
            field "total" type="float64" nullable=#false
        }}
    }}
    sink "legacy_out" connector="file" component="Order" codec="legacy_csv" {{
        config {{
            path "{d}/legacy_out.csv"
            truncate #true
            field "id" type="int64" nullable=#false
            field "total" type="float64" nullable=#false
        }}
    }}
    link from="legacy_in" to="legacy_out"
}}

http bind="127.0.0.1:0" control=#true
observability log_level="warn" {{
    inspector enabled=#true
}}
"#
    );

    let cfg_path = dir.path().join("service.kdl");
    std::fs::write(&cfg_path, &kdl_v1).expect("write v1");

    let mut service = Service::spawn(&cfg_path);
    let health_url = service.url("/health");
    service
        .poll_until_200(&health_url, STARTUP_BUDGET)
        .await
        .expect("service starts");

    let client = probe_client();
    let topo1: serde_json::Value = client
        .get(service.url("/api/topology"))
        .send()
        .await
        .expect("GET topology")
        .json()
        .await
        .expect("topology JSON");
    let v1 = topo1["version"].as_u64().expect("version u64");

    // 1. Invalid reload returns 400 and leaves running workflows untouched.
    let bad_resp = client
        .post(service.url("/api/reload"))
        .body("not valid kdl {{{")
        .send()
        .await
        .expect("POST bad reload");
    assert_eq!(bad_resp.status(), 400);

    // 2. Valid HTTP reload removes `legacy`, adds `added_wf`, leaves `steady` uninterrupted.
    let kdl_v2 = format!(
        r#"
mode "standalone"
node id=1 name="reload-node" data_dir="{d}"
run "continuous"

workflow "steady" {{
    codec "steady_csv" format="csv" {{ config has_headers=#true }}
    source "steady_in" connector="file" component="Order" codec="steady_csv" {{
        config {{
            path "{d}/orders.csv"
            field "id" type="int64" nullable=#false
            field "total" type="float64" nullable=#false
        }}
    }}
    sink "steady_out" connector="file" component="Order" codec="steady_csv" {{
        config {{
            path "{d}/steady_out.csv"
            truncate #true
            field "id" type="int64" nullable=#false
            field "total" type="float64" nullable=#false
        }}
    }}
    link from="steady_in" to="steady_out"
}}

workflow "added_wf" {{
    codec "added_csv" format="csv" {{ config has_headers=#true }}
    source "added_in" connector="file" component="Order" codec="added_csv" {{
        config {{
            path "{d}/orders.csv"
            field "id" type="int64" nullable=#false
            field "total" type="float64" nullable=#false
        }}
    }}
    sink "added_out" connector="file" component="Order" codec="added_csv" {{
        config {{
            path "{d}/added_out.csv"
            truncate #true
            field "id" type="int64" nullable=#false
            field "total" type="float64" nullable=#false
        }}
    }}
    link from="added_in" to="added_out"
}}

http bind="127.0.0.1:0" control=#true
observability log_level="warn" {{
    inspector enabled=#true
}}
"#
    );

    let reload_resp = client
        .post(service.url("/api/reload"))
        .body(kdl_v2)
        .send()
        .await
        .expect("POST reload");
    assert_eq!(reload_resp.status(), 200);
    let report: serde_json::Value = reload_resp.json().await.expect("reload JSON");
    assert_eq!(report["added"], serde_json::json!(["added_wf"]));
    assert_eq!(report["removed"], serde_json::json!(["legacy"]));
    assert_eq!(report["unchanged"], serde_json::json!(["steady"]));
    let v2 = report["topology_version"].as_u64().expect("version u64");
    assert!(v2 > v1, "expected topology version bump: v1={v1}, v2={v2}");

    let steady_wf: serde_json::Value = client
        .get(service.url("/api/workflows/steady"))
        .send()
        .await
        .expect("GET steady")
        .json()
        .await
        .expect("steady JSON");
    assert_eq!(steady_wf["runs"], 1, "unchanged workflow was not rebuilt");

    // 3. Unix SIGHUP reloads the updated file on disk.
    #[cfg(unix)]
    {
        let kdl_v3 = format!(
            r#"
mode "standalone"
node id=1 name="reload-node" data_dir="{d}"
run "continuous"

workflow "steady" {{
    codec "steady_csv" format="csv" {{ config has_headers=#true }}
    source "steady_in" connector="file" component="Order" codec="steady_csv" {{
        config {{
            path "{d}/orders.csv"
            field "id" type="int64" nullable=#false
            field "total" type="float64" nullable=#false
        }}
    }}
    sink "steady_out" connector="file" component="Order" codec="steady_csv" {{
        config {{
            path "{d}/steady_out.csv"
            truncate #true
            field "id" type="int64" nullable=#false
            field "total" type="float64" nullable=#false
        }}
    }}
    link from="steady_in" to="steady_out"
}}

workflow "sighup_wf" {{
    codec "sighup_csv" format="csv" {{ config has_headers=#true }}
    source "sighup_in" connector="file" component="Order" codec="sighup_csv" {{
        config {{
            path "{d}/orders.csv"
            field "id" type="int64" nullable=#false
            field "total" type="float64" nullable=#false
        }}
    }}
    sink "sighup_out" connector="file" component="Order" codec="sighup_csv" {{
        config {{
            path "{d}/sighup_out.csv"
            truncate #true
            field "id" type="int64" nullable=#false
            field "total" type="float64" nullable=#false
        }}
    }}
    link from="sighup_in" to="sighup_out"
}}

http bind="127.0.0.1:0" control=#true
observability log_level="warn" {{
    inspector enabled=#true
}}
"#
        );
        std::fs::write(&cfg_path, &kdl_v3).expect("write v3");
        let pid = service.child.id().to_string();
        let status = std::process::Command::new("kill")
            .args(["-HUP", &pid])
            .status()
            .expect("send SIGHUP");
        assert!(status.success(), "kill -HUP succeeded");

        let deadline = Instant::now() + STARTUP_BUDGET;
        loop {
            let resp = client
                .get(service.url("/api/workflows/sighup_wf"))
                .send()
                .await
                .expect("GET sighup_wf");
            if resp.status() == 200 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for sighup_wf after SIGHUP"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    service.terminate();
    service.wait_exit(SHUTDOWN_BUDGET);
}
