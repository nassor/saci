//! Both UDP halves in one config: a `udp` source receiving datagrams and a
//! `udp` sink sending them, over one component, driven by the stream runner.
//!
//! One datagram carries one message in either direction, so the rows a
//! producer sends into the source come back out of the sink payload for
//! payload.
//!
//! The second exercise is what a fire-and-forget sink needs the host for. A
//! peer that stops answering is invisible to `send`, so the sink's liveness
//! probe is what notices, and a batch no peer accepted is the `write_batch`
//! error the `heal` layer acts on. Here the peer stops echoing, the sink is
//! rebuilt, and rows resume once an echoing peer is back on the same port.

#![cfg(all(
    feature = "service",
    feature = "connector-udp",
    feature = "transformer-arrow-ipc",
    feature = "wasm"
))]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use tokio::net::UdpSocket;
use tokio::task::LocalSet;
use tokio_util::sync::CancellationToken;

use saci_connector::{ConfigValue, ConnectorContext};
use saci_connector_udp::UdpSinkFactory;
use saci_core::error::SaciError;
use saci_core::io::sink::Sink;
use saci_core::io::source::Source;
use saci_core::pipeline::Pipeline;
use saci_service::service::builder::ServiceBuilder;
use saci_service::service::config::ServiceConfig;
use saci_service::service::factories::register_builtin_factories;
use saci_service::service::registry::{SinkFactory, SourceFactory};
use saci_service::service::run_standalone;
use saci_transformer::Transformer;
use saci_transformer_arrow_ipc::ArrowIpcTransformer;

const COMPONENT: &str = "Tick";

/// How long the heal exercise may take before the test calls it a failure.
/// Generous: every wait below is on a real socket.
const BUDGET: Duration = Duration::from_secs(20);

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
}

fn build_pipeline(name: &str) -> Pipeline {
    let mut pipeline = Pipeline::new(name);
    pipeline.data.register_raw_component(COMPONENT, schema());
    pipeline
}

/// One datagram: one Arrow IPC stream payload, with no prefix of any kind.
fn datagram(values: &[i64]) -> Vec<u8> {
    let batch = RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(values.to_vec()))])
        .expect("build batch");
    let mut payload: Vec<u8> = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut payload, &schema()).expect("ipc writer");
        writer.write(&batch).expect("write batch");
        writer.finish().expect("finish stream");
    }
    payload
}

/// Decode a datagram through the same transformer the config names.
fn decode(payload: &[u8]) -> RecordBatch {
    let mut decoder = ArrowIpcTransformer::new()
        .open_message_decoder(schema())
        .expect("decoder");
    decoder.push(payload).expect("push");
    decoder.flush().expect("flush").expect("one batch")
}

fn values(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("v is Int64")
        .values()
        .to_vec()
}

/// A quoted KDL string reads backslashes as escapes, so a Windows path has to
/// go in with forward slashes.
fn config_path_text(path: &std::path::Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

/// A free port for the source's `bind`, reserved on a datagram socket so the
/// factory can bind one there.
fn reserved_udp_port() -> u16 {
    let probe = std::net::UdpSocket::bind("127.0.0.1:0").expect("probe bind");
    let port = probe.local_addr().expect("probe addr").port();
    drop(probe);
    port
}

fn roundtrip_kdl(bind: &str, connect: &str, data_dir: &str) -> String {
    format!(
        r#"
mode "standalone"

node id=1 name="saci-udp-connector" data_dir="{data_dir}"

run_mode kind="stream"

workflow "udp-connector-test" {{
    transformer "ipc" format="arrow-ipc"

    source "datagrams_in" type="udp" component="{COMPONENT}" transformer="ipc" {{
        config {{
            bind "{bind}"
            buffer 8
            schema_fields "v" type="int64" nullable=#false
        }}
    }}

    wasm "relay" name="Relay"

    sink "datagrams_out" type="udp" component="{COMPONENT}" transformer="ipc" {{
        config {{
            connect "{connect}"
            schema_fields "v" type="int64" nullable=#false
        }}
    }}

    link from="datagrams_in" to="relay"
    link from="relay" to="datagrams_out"
}}

http disabled=#true

observability log_level="warn"
"#
    )
}

/// Receive one datagram, or fail naming what was waited on.
async fn recv_datagram(socket: &UdpSocket, what: &str) -> Vec<u8> {
    let mut buf = vec![0u8; u16::MAX as usize];
    let (n, _from) = tokio::time::timeout(Duration::from_secs(10), socket.recv_from(&mut buf))
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
        .expect("recv_from");
    buf.truncate(n);
    buf
}

#[tokio::test]
async fn datagrams_pushed_into_the_udp_source_come_back_out_of_the_udp_sink() {
    // The sink's peer: bound here so the config can name a real port. It never
    // answers, and with no `probe` block the sink never asks.
    let collector = UdpSocket::bind("127.0.0.1:0").await.expect("bind");
    let collector_addr = collector.local_addr().expect("collector addr");
    let bind_addr = format!("127.0.0.1:{}", reserved_udp_port());

    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = dir.path().join("service.kdl");
    std::fs::write(
        &config_path,
        roundtrip_kdl(
            &bind_addr,
            &collector_addr.to_string(),
            &config_path_text(dir.path()),
        ),
    )
    .expect("write config");

    let config = ServiceConfig::load(&config_path).expect("config loads");
    let built = register_builtin_factories(ServiceBuilder::new())
        .with_runtime("relay", Box::new(build_pipeline("udp_connector")))
        .build_all(&config)
        .expect("both udp halves resolve through the registry")
        .remove(0);

    let cancel = CancellationToken::new();
    let runner_cancel = cancel.clone();
    let runner_config = config.clone();
    let local = LocalSet::new();
    let handle = local.spawn_local(async move {
        run_standalone(built, &runner_config, runner_cancel, None, None).await
    });

    let stats = local
        .run_until(async move {
            // The socket is already bound by the factory, so the kernel holds
            // these datagrams until the receive loop starts.
            let producer = UdpSocket::bind("127.0.0.1:0").await.expect("producer bind");
            producer
                .send_to(&datagram(&[1, 2]), &bind_addr)
                .await
                .expect("datagram one");
            producer
                .send_to(&datagram(&[3]), &bind_addr)
                .await
                .expect("datagram two");

            let first = decode(&recv_datagram(&collector, "the first datagram out").await);
            let second = decode(&recv_datagram(&collector, "the second datagram out").await);
            assert_eq!(values(&first), vec![1, 2], "first item's rows, in order");
            assert_eq!(values(&second), vec![3], "one datagram per item");

            drop(producer);
            cancel.cancel();
            handle.await.expect("runner task").expect("stream run")
        })
        .await;

    assert_eq!(stats.iterations, 2, "one item per received datagram");
    assert_eq!(stats.iteration_errors, 0);
}

fn validation_kdl(run_mode: &str, source_type: &str) -> String {
    format!(
        r#"
mode "standalone"

node id=1 data_dir="/tmp/saci-udp-validate"

run_mode {run_mode}

workflow "w" {{
    source "in" type="{source_type}" component="{COMPONENT}"
    sink "out" type="udp" component="{COMPONENT}"
    link from="in" to="out"
}}
"#
    )
}

fn load_from_text(dir: &tempfile::TempDir, name: &str, text: &str) -> saci_core::SaciResult<()> {
    let path = dir.path().join(name);
    std::fs::write(&path, text).expect("write config");
    ServiceConfig::load(&path).map(|_| ())
}

/// The no-EOF rule reads `source` nodes, so a `udp` sink is legal in every run
/// mode even though a `udp` source is stream-only.
#[test]
fn a_udp_sink_is_accepted_outside_stream_mode() {
    let dir = tempfile::tempdir().expect("tempdir");
    for run_mode in [
        r#"kind="one_shot""#,
        r#"kind="interval" interval_ms=1000"#,
        r#"kind="stream""#,
    ] {
        load_from_text(
            &dir,
            "sink_only.kdl",
            &validation_kdl(run_mode, "NoopSource"),
        )
        .unwrap_or_else(|e| panic!("a udp sink under {run_mode} must validate: {e}"));
    }
}

/// The mirror of the rule above: the source half is still stream-only.
#[test]
fn a_udp_source_is_still_rejected_outside_stream_mode() {
    let dir = tempfile::tempdir().expect("tempdir");
    let err = load_from_text(
        &dir,
        "source_one_shot.kdl",
        &validation_kdl(r#"kind="one_shot""#, "udp"),
    )
    .expect_err("a udp source outside stream mode must fail");
    assert_eq!(err.category(), "configuration", "got: {err}");
    assert!(err.to_string().contains("never reaches EOF"), "got: {err}");
}

// ── The heal exercise ────────────────────────────────────────────────────────

/// A source that never stops producing, one row per poll, so the stream runner
/// keeps handing the sink work for as long as the test needs.
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
        // Paced, so a sink whose peer is down does not spin this loop at full
        // speed while the test is waiting on a socket.
        tokio::time::sleep(Duration::from_millis(5)).await;
        self.next += 1;
        Ok(Some(
            RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vec![self.next]))])
                .expect("one-row batch"),
        ))
    }
}

struct TickSourceFactory;

impl SourceFactory for TickSourceFactory {
    fn type_name(&self) -> &'static str {
        "tick"
    }

    fn build(
        &self,
        _config: &ConfigValue,
        _ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        Ok(Box::new(TickSource { next: 0 }))
    }
}

/// The real [`UdpSinkFactory`] behind a build counter, which is what makes a
/// rebuild observable from the test.
struct CountingUdpSinkFactory(Arc<AtomicUsize>);

impl SinkFactory for CountingUdpSinkFactory {
    fn type_name(&self) -> &'static str {
        "udp-counted"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        UdpSinkFactory.build(config, ctx)
    }

    fn rebuildable(&self, config: &ConfigValue) -> Result<(), &'static str> {
        UdpSinkFactory.rebuildable(config)
    }
}

/// A peer socket that answers every liveness probe and counts the data
/// datagrams it received.
struct Peer {
    data: Arc<AtomicUsize>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Peer {
    /// Serve `socket`, echoing every zero-length datagram the way a `udp`
    /// source does.
    fn serve(socket: UdpSocket) -> Self {
        let data = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&data);
        let task = tokio::spawn(async move {
            let mut buf = vec![0u8; u16::MAX as usize];
            loop {
                match socket.recv_from(&mut buf).await {
                    Ok((0, from)) => {
                        let _ = socket.send_to(&[], from).await;
                    }
                    Ok(_) => {
                        counter.fetch_add(1, Ordering::SeqCst);
                    }
                    // A stale ICMP report from an earlier send surfaces on the
                    // next receive on Windows; it says nothing about this
                    // socket.
                    Err(e)
                        if matches!(
                            e.kind(),
                            std::io::ErrorKind::ConnectionReset
                                | std::io::ErrorKind::ConnectionRefused
                        ) => {}
                    Err(_) => return,
                }
            }
        });
        Self {
            data,
            task: Some(task),
        }
    }

    /// Take the peer away: the task ends and its socket drops with it, so the
    /// port is closed and nothing answers a probe.
    async fn stop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }

    fn received(&self) -> usize {
        self.data.load(Ordering::SeqCst)
    }
}

fn heal_kdl(connect: &str, data_dir: &str) -> String {
    format!(
        r#"
mode "standalone"

node id=1 name="saci-udp-heal" data_dir="{data_dir}"

run_mode kind="stream"

// Only the schedule: no `enabled` key anywhere, so the sink heals under the
// default policy.
heal {{
    after_failures 1
    base_delay_ms 20
    max_delay_ms 40
}}

workflow "udp-heal-test" {{
    transformer "ipc" format="arrow-ipc"

    source "ticks" type="tick" component="{COMPONENT}" {{
        config {{
            schema_fields "v" type="int64" nullable=#false
        }}
    }}

    sink "collector" type="udp-counted" component="{COMPONENT}" transformer="ipc" {{
        // One attempt: a peer that failed its probe refuses every write, so
        // the failure must reach the heal layer rather than be retried.
        retry max_attempts=1
        config {{
            connect "{connect}"
            probe kind="echo" interval_ms=100 timeout_ms=100 on_failure="refuse"
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
async fn a_udp_sink_whose_peer_stops_echoing_is_rebuilt_and_resumes_once_it_echoes_again() {
    let socket = UdpSocket::bind("127.0.0.1:0").await.expect("bind");
    let addr = socket.local_addr().expect("addr");
    let mut first = Peer::serve(socket);

    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = dir.path().join("service.kdl");
    std::fs::write(
        &config_path,
        heal_kdl(&addr.to_string(), &config_path_text(dir.path())),
    )
    .expect("write config");

    let config = ServiceConfig::load(&config_path).expect("config loads");
    let sink_builds = Arc::new(AtomicUsize::new(0));
    let built = register_builtin_factories(ServiceBuilder::new())
        .register_source(TickSourceFactory)
        .register_sink(CountingUdpSinkFactory(Arc::clone(&sink_builds)))
        .build_all(&config)
        .expect("the workflow builds")
        .remove(0);

    let cancel = CancellationToken::new();
    let runner_cancel = cancel.clone();
    let runner_config = config.clone();
    let local = LocalSet::new();
    let handle = local.spawn_local(async move {
        run_standalone(built, &runner_config, runner_cancel, None, None).await
    });

    let stats = local
        .run_until(async move {
            // 1. The peer echoes the probe, so rows flow.
            wait_for(&first.data, 3, "the first peer to receive datagrams").await;

            // 2. The peer goes away. Its port is closed, so nothing answers
            //    the probe and every write is refused.
            first.stop().await;
            let delivered_before = first.received();

            // 3. The probe marks the peer down, the refused writes reach the
            //    heal layer, and the sink is rebuilt.
            wait_for(&sink_builds, 2, "the first rebuild").await;

            // 4. An echoing peer comes back on the same port. The instance
            //    that was live before it left is already gone.
            let reopened = loop {
                match UdpSocket::bind(addr).await {
                    Ok(socket) => break socket,
                    Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
                }
            };
            let mut second = Peer::serve(reopened);

            // 5. The proof: datagrams reach the second peer.
            wait_for(&second.data, 3, "the rebuilt sink to reach the second peer").await;

            assert_eq!(
                first.received(),
                delivered_before,
                "the first peer is gone and receives nothing more"
            );

            second.stop().await;
            cancel.cancel();
            (
                handle.await.expect("runner task").expect("stream run"),
                sink_builds.load(Ordering::SeqCst),
            )
        })
        .await;

    let (stats, builds) = stats;
    assert!(
        builds >= 2,
        "the sink must have been rebuilt at least once, saw {builds} build(s)"
    );
    assert!(
        stats.iteration_errors > 0,
        "the refused writes between the peer leaving and the heal must be counted"
    );
}
