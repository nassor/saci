//! A connector that cannot reconnect on its own is replaced by the host.
//!
//! `TcpSink` holds one socket for its whole life: once the peer goes away,
//! every later write fails on that same dead socket, and re-driving the call
//! cannot help. This drives a real one against a real listener, takes the
//! listener away, brings it back on the same port, and asserts rows resume.
//!
//! No `heal` block enables anything here: the config only shortens the
//! schedule, so what heals the sink is the default policy.

#![cfg(all(
    feature = "service",
    feature = "connector-tcp",
    feature = "transformer-arrow-ipc"
))]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::task::LocalSet;
use tokio_util::sync::CancellationToken;

use saci_connector::{ConfigValue, ConnectorContext};
use saci_core::error::SaciError;
use saci_core::io::source::Source;
use saci_service::service::builder::ServiceBuilder;
use saci_service::service::config::ServiceConfig;
use saci_service::service::factories::register_builtin_factories;
use saci_service::service::registry::SourceFactory;
use saci_service::service::run_standalone;

const COMPONENT: &str = "Tick";

/// How long the whole exercise may take before the test calls it a failure.
/// Generous: every wait below is on a real socket.
const BUDGET: Duration = Duration::from_secs(20);

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
}

/// A source that never stops producing, one row per poll, so the stream
/// runner keeps handing the sink work for as long as the test needs.
///
/// It is deliberately not healable (it keeps the factory trait's default
/// answer), so the only node this test can heal is the sink.
struct TickSource {
    next: i64,
}

#[async_trait]
impl Source for TickSource {
    fn schema(&self) -> Arc<Schema> {
        schema()
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        // Paced, so a sink that is down does not spin this loop at full speed
        // while the test is waiting on a socket.
        tokio::time::sleep(Duration::from_millis(5)).await;
        self.next += 1;
        Ok(Some(
            RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vec![self.next]))])
                .expect("one-row batch"),
        ))
    }
}

struct TickSourceFactory(Arc<AtomicUsize>);

impl SourceFactory for TickSourceFactory {
    fn type_name(&self) -> &'static str {
        "tick"
    }

    fn build(
        &self,
        _config: &ConfigValue,
        _ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(TickSource { next: 0 }))
    }
}

/// A quoted KDL string reads backslashes as escapes, so a Windows path has to
/// go in with forward slashes.
fn config_path_text(path: &std::path::Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn config_kdl(connect: &str, data_dir: &str) -> String {
    format!(
        r#"
mode "standalone"

node id=1 name="saci-connector-heal" data_dir="{data_dir}"

run_mode kind="stream"

// Only the schedule: no `enabled` key anywhere, so the sink heals under the
// default policy.
heal {{
    after_failures 1
    base_delay_ms 20
    max_delay_ms 40
}}

workflow "heal-test" {{
    transformer "ipc" format="arrow-ipc"

    source "ticks" type="tick" component="{COMPONENT}" {{
        config {{
            schema_fields "v" type="int64" nullable=#false
        }}
    }}

    sink "collector" type="tcp" component="{COMPONENT}" transformer="ipc" {{
        // One attempt: re-driving a dead socket cannot help, so the failure
        // must reach the heal layer rather than be retried seven times.
        retry max_attempts=1
        config {{
            connect "{connect}"
            schema_fields "v" type="int64" nullable=#false
        }}
    }}

    link from="ticks" to="collector"
}}

http disabled=#true

observability log_level="warn"
"#
    )
}

/// Accept one connection and read until it breaks, counting frames.
///
/// Aborting the returned handle drops the accepted stream, which is what
/// takes the sink's peer away.
fn serve(listener: TcpListener, frames: Arc<AtomicUsize>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let Ok((mut stream, _)) = listener.accept().await else {
            return;
        };
        let mut header = [0u8; 4];
        loop {
            if stream.read_exact(&mut header).await.is_err() {
                return;
            }
            let len = u32::from_be_bytes(header) as usize;
            let mut payload = vec![0u8; len];
            if stream.read_exact(&mut payload).await.is_err() {
                return;
            }
            frames.fetch_add(1, Ordering::SeqCst);
        }
    })
}

/// Wait until `counter` reaches `target`, or fail naming what was waited on.
async fn wait_for(counter: &Arc<AtomicUsize>, target: usize, what: &str) {
    let deadline = tokio::time::Instant::now() + BUDGET;
    while counter.load(Ordering::SeqCst) < target {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}: reached {} of {target}",
            counter.load(Ordering::SeqCst)
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn a_tcp_sink_whose_peer_disappears_is_rebuilt_and_resumes_writing() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");

    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = dir.path().join("service.kdl");
    std::fs::write(
        &config_path,
        config_kdl(&addr.to_string(), &config_path_text(dir.path())),
    )
    .expect("write config");

    let config = ServiceConfig::load(&config_path).expect("config loads");
    let source_builds = Arc::new(AtomicUsize::new(0));
    let built = register_builtin_factories(ServiceBuilder::new())
        .register_source(TickSourceFactory(Arc::clone(&source_builds)))
        .build_all(&config)
        .expect("the workflow builds")
        .remove(0);

    let before = Arc::new(AtomicUsize::new(0));
    let first = serve(listener, Arc::clone(&before));

    let cancel = CancellationToken::new();
    let runner_cancel = cancel.clone();
    let runner_config = config.clone();
    let local = LocalSet::new();
    let handle = local.spawn_local(async move {
        run_standalone(built, &runner_config, runner_cancel, None, None).await
    });

    let stats = local
        .run_until(async move {
            // 1. The sink dials the listener and rows flow.
            wait_for(&before, 3, "the first listener to receive frames").await;

            // 2. The peer disappears. `TcpSink` documents that it never
            //    reconnects: every write after this fails on the same socket.
            first.abort();
            let _ = first.await;
            let delivered_before = before.load(Ordering::SeqCst);

            // 3. The peer comes back on the same port. Nothing in the sink
            //    reacts to that; only a rebuilt one dials again.
            let after = Arc::new(AtomicUsize::new(0));
            let reopened = loop {
                match TcpListener::bind(addr).await {
                    Ok(listener) => break listener,
                    Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
                }
            };
            let second = serve(reopened, Arc::clone(&after));

            // 4. The proof: frames reach the second listener.
            wait_for(&after, 3, "the rebuilt sink to reach the second listener").await;

            assert_eq!(
                before.load(Ordering::SeqCst),
                delivered_before,
                "the first listener is gone and receives nothing more"
            );

            second.abort();
            let _ = second.await;
            cancel.cancel();
            handle.await.expect("runner task").expect("stream run")
        })
        .await;

    assert!(
        stats.iteration_errors > 0,
        "the writes between the peer going away and the heal must be counted"
    );
    assert_eq!(
        source_builds.load(Ordering::SeqCst),
        1,
        "the source never failed, so it was never rebuilt"
    );
}
