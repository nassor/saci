//! The inspector's HTTP contract, over a real socket.
//!
//! Its own test binary because it installs a meter provider, which is a
//! process-global one-shot: sharing the lib test binary would let whichever
//! test ran first decide what the instruments record into.
//!
//! What is exercised end to end:
//!
//! - `/api/topology` shape: node and edge counts from a real config fixture.
//! - Credential redaction: the fixture carries a NATS password and a Postgres
//!   DSN, and neither may appear anywhere in any response.
//! - `/api/snapshot`: series populated from a real `SdkMeterProvider` collect,
//!   and `buffers.spans` populated from a real pipeline run under the capture
//!   layer.
//! - `/ui`: the embedded dashboard, with the mount point the WASM entry point
//!   looks up.
//! - A disabled inspector: `/api/*` and `/ui` 404 while `/health` still answers.
//!
//! ```text
//! cargo test --test inspector_http -p saci-service --all-features
//! ```

#![cfg(all(feature = "service", feature = "inspector"))]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::time::{Duration, Instant};

use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::prelude::*;

use saci_connector_channel::{ChannelSink, ChannelSource};
use saci_inspector_wire::{Snapshot, Topology};
use saci_service::inspector::{Inspector, InspectorConfig};
use saci_service::prelude::*;
use saci_service::service::builder::{BuiltEdge, BuiltNode, BuiltNodeKind};
use saci_service::service::config::ServiceConfig;
use saci_service::service::http::{ServiceModeLabel, ServiceState};
use saci_service::service::{build_router, build_topology};

/// Two sources and one sink, with a password in one and a DSN in the other.
///
/// Neither connector is built here: the topology comes from a hand-assembled
/// `BuiltNode` list, matching this shape, plus the parsed config, which is
/// exactly what `ServiceBuilder::build_all` assembles for real.
const FIXTURE: &str = r#"
mode "standalone"

node id=42 data_dir="/tmp/saci-inspector-http"

workflow "inspector-http" {
    transformer "ndjson_fmt" format="ndjson"

    source "orders-eu" type="NatsSource" component="Reading" transformer="ndjson_fmt" {
        config {
            mode kind="core" subject="authorizations.eu"
            connection url="nats://localhost:4222" password="hunter2-eu"
        }
    }

    source "orders-us" type="NatsSource" component="Reading" transformer="ndjson_fmt" {
        config {
            mode kind="core" subject="authorizations.us"
            connection url="nats://localhost:4222" password="hunter2-us"
        }
    }

    wasm "scale" name="Scale"

    sink "settlements" type="PostgresSink" component="Reading" {
        config {
            table "public.settlements"
            write_mode "upsert"
            connection dsn="postgres://postgres:s3cret@127.0.0.1:5432/saci"
        }
    }

    link from="orders-eu" to="scale"
    link from="orders-us" to="scale"
    link from="scale" to="settlements"
}
"#;

#[derive(Serialize, Deserialize)]
struct Reading {
    raw: f64,
    scaled: f64,
}

impl Component for Reading {
    fn name() -> &'static str {
        "Reading"
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("raw", DataType::Float64, false),
            Field::new("scaled", DataType::Float64, false),
        ]))
    }
}

/// Writes `Reading.scaled`, so the run opens a `pipeline.stage` span with a
/// `stage` field for the inspector to group on.
struct Scale;

#[async_trait]
impl System for Scale {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("scale")
            .read("Reading", "raw")
            .write("Reading", "scaled")
    }

    async fn run(&self, _data: &mut Dataset) -> SaciResult<()> {
        Ok(())
    }
}

/// Build a state whose router carries the inspector routes.
fn state_with(inspector: Option<Inspector>) -> ServiceState {
    ServiceState {
        node_id: 42,
        node_name: Some("inspector-http".to_string()),
        mode: ServiceModeLabel::Standalone,
        started_at: Instant::now(),
        prometheus_registry: Arc::new(prometheus::Registry::new()),
        liveness: Arc::new(AtomicU64::new(0)),
        ready: Arc::new(AtomicBool::new(true)),
        cluster_probe: None,
        standalone_stats: None,
        inspector,
        lifecycle: None,
        dlq: None,
    }
}

/// Bind the router on an ephemeral port.
async fn serve(state: ServiceState) -> (String, CancellationToken) {
    let cancel = CancellationToken::new();
    // The tests drive the router with a reqwest client, which 0.13 refuses to
    // build without an installed crypto provider.
    saci_service::service::install_ring_provider();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr").to_string();

    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        let router = build_router(state);
        let _ = axum::serve(listener, router)
            .with_graceful_shutdown(async move { cancel_clone.cancelled().await })
            .await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    (addr, cancel)
}

#[tokio::test(flavor = "multi_thread")]
async fn inspector_api_serves_topology_snapshot_and_dashboard() {
    let config: ServiceConfig = serde::Deserialize::deserialize(
        saci_service::service::config::from_kdl_str(FIXTURE).expect("fixture parses"),
    )
    .expect("fixture deserializes");

    let inspector = Inspector::new(&InspectorConfig::default());

    // A real provider with a real PeriodicReader on the inspector's exporter:
    // `force_flush` then drives one genuine export, so `series` below is what
    // the SDK aggregated rather than a hand-built sample.
    let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_reader(
            opentelemetry_sdk::metrics::PeriodicReader::builder(inspector.metric_exporter())
                .with_interval(Duration::from_secs(3600))
                .build(),
        )
        .build();
    opentelemetry::global::set_meter_provider(provider.clone());
    saci_service::metrics::init();

    let pipeline = Pipeline::builder("inspector-http")
        .with::<Reading>()
        .with_system(Scale)
        .build();

    // A `BuiltNode` list matching `FIXTURE`'s shape, standing in for what
    // `ServiceBuilder::build_all` would assemble from real connectors. The
    // processor node owns a second, identically-built `Pipeline`: `build_topology`
    // takes ownership through `BuiltNode`, so the instance actually driven
    // below by `run_on` has to be a separate one.
    let reading_schema = Reading::schema();
    let (_tx_eu, src_eu) = ChannelSource::new(Arc::clone(&reading_schema), 1);
    let (_tx_us, src_us) = ChannelSource::new(Arc::clone(&reading_schema), 1);
    let (sink, _rx) = ChannelSink::new(Arc::clone(&reading_schema), 1);
    let topology_pipeline = Pipeline::builder("inspector-http")
        .with::<Reading>()
        .with_system(Scale)
        .build();
    let nodes = vec![
        BuiltNode {
            id: "orders-eu".to_string(),
            name: None,
            type_name: "NatsSource".to_string(),
            component: Some("Reading"),
            kind: BuiltNodeKind::Source(Box::new(src_eu)),
            downstream: vec![BuiltEdge {
                node: 2,
                branch: None,
            }],
            artifact: None,
            #[cfg(feature = "windows")]
            window: None,
            heal_recovered: None,
        },
        BuiltNode {
            id: "orders-us".to_string(),
            name: None,
            type_name: "NatsSource".to_string(),
            component: Some("Reading"),
            kind: BuiltNodeKind::Source(Box::new(src_us)),
            downstream: vec![BuiltEdge {
                node: 2,
                branch: None,
            }],
            artifact: None,
            #[cfg(feature = "windows")]
            window: None,
            heal_recovered: None,
        },
        BuiltNode {
            id: "scale".to_string(),
            name: Some("Scale".to_string()),
            type_name: "native".to_string(),
            component: None,
            kind: BuiltNodeKind::Processor {
                runtime: Box::new(topology_pipeline),
                kind: "native",
            },
            downstream: vec![BuiltEdge {
                node: 3,
                branch: None,
            }],
            artifact: None,
            #[cfg(feature = "windows")]
            window: None,
            heal_recovered: None,
        },
        BuiltNode {
            id: "settlements".to_string(),
            name: None,
            type_name: "PostgresSink".to_string(),
            component: Some("Reading"),
            kind: BuiltNodeKind::Sink(Box::new(sink)),
            downstream: Vec::new(),
            artifact: None,
            #[cfg(feature = "windows")]
            window: None,
            heal_recovered: None,
        },
    ];

    inspector.set_topology(build_topology(&config, &[nodes.as_slice()], 1));

    let mut dataset = Dataset::new();
    dataset
        .register_component::<Reading>()
        .expect("register Reading");
    dataset
        .append::<Reading>(&[Reading {
            raw: 1.0,
            scaled: 0.0,
        }])
        .expect("append Reading");

    // The capture layer is installed for the run only, so this test needs no
    // global subscriber and cannot race the lib tests for one.
    // `SpanMetricsLayer` sits on the same subscriber: closing the run's
    // `pipeline.stage` span records `saci_stage_duration_seconds` through the
    // service's own writer, so `/api/snapshot` reports a series the production
    // path produced rather than one this test injected.
    let subscriber = tracing_subscriber::registry()
        .with(inspector.layer())
        .with(saci_service::service::SpanMetricsLayer);
    tracing::subscriber::with_default(subscriber, || {
        futures::executor::block_on(pipeline.run_on(&mut dataset)).expect("pipeline run");
        // One event inside the run's span tree, so the log buffer and the
        // trace/log correlation are exercised too.
        tracing::info!(rows = 1, "inspector-http fixture run");
    });

    provider.force_flush().expect("collect metrics");

    let (addr, cancel) = serve(state_with(Some(inspector))).await;
    let client = reqwest::Client::new();

    // ── /api/topology ────────────────────────────────────────────────────────
    let response = client
        .get(format!("http://{addr}/api/topology"))
        .send()
        .await
        .expect("GET /api/topology");
    assert_eq!(response.status(), 200);
    let body = response.text().await.expect("topology body");

    assert!(
        !body.contains("hunter2-eu") && !body.contains("hunter2-us"),
        "NATS password leaked into /api/topology: {body}"
    );
    assert!(
        !body.contains("s3cret") && !body.contains("dsn"),
        "Postgres DSN leaked into /api/topology: {body}"
    );

    let topology: Topology = serde_json::from_str(&body).expect("topology decodes");
    assert_eq!(topology.node_id, "42");
    assert_eq!(topology.mode, "standalone");
    let workflow_topology = &topology.workflows[0];
    assert_eq!(
        workflow_topology.nodes.len(),
        4,
        "two sources, one processor, one sink: {:?}",
        workflow_topology.nodes
    );
    assert_eq!(
        workflow_topology
            .nodes
            .iter()
            .filter(|node| node.kind == "source")
            .count(),
        2
    );
    assert_eq!(
        workflow_topology
            .nodes
            .iter()
            .filter(|node| node.kind == "processor")
            .count(),
        1
    );
    assert_eq!(
        workflow_topology.edges.len(),
        3,
        "two source edges and one sink edge: {:?}",
        workflow_topology.edges
    );
    let processor_node = workflow_topology
        .nodes
        .iter()
        .find(|node| node.kind == "processor")
        .expect("processor node");
    assert!(
        processor_node
            .runtime
            .as_ref()
            .expect("processor runtime info")
            .declared_components
            .contains(&"Reading".to_string())
    );

    // ── /api/snapshot ────────────────────────────────────────────────────────
    let response = client
        .get(format!("http://{addr}/api/snapshot?window_secs=60"))
        .send()
        .await
        .expect("GET /api/snapshot");
    assert_eq!(response.status(), 200);
    let snapshot: Snapshot = response.json().await.expect("snapshot decodes");

    assert_eq!(snapshot.topology_version, 1);
    assert!(snapshot.ready);
    assert!(
        !snapshot.series.is_empty(),
        "one collect must produce at least one series"
    );
    assert!(
        snapshot.buffers.spans > 0,
        "the pipeline run must have left spans behind"
    );
    assert!(
        snapshot.buffers.logs > 0,
        "the event emitted inside the run must have been captured"
    );

    // Neither stub source nor the stub sink is ever drained in this test:
    // `run_on` is called directly against a hand-populated dataset. So no
    // `saci_rows_processed_total`/`saci_sink_batches_written_total` sample
    // exists yet, and every edge is correctly omitted rather than reported
    // as a stand-in zero.
    assert!(
        snapshot.edges.is_empty(),
        "no source or sink was ever drained: {:?}",
        snapshot.edges
    );

    // ── /api/traces and /api/logs ────────────────────────────────────────────
    let response = client
        .get(format!("http://{addr}/api/traces?limit=10"))
        .send()
        .await
        .expect("GET /api/traces");
    assert_eq!(response.status(), 200);
    let traces: Vec<saci_inspector_wire::TraceSummary> =
        response.json().await.expect("traces decode");
    assert!(
        !traces.is_empty(),
        "a native runtime's pipeline.run span is a trace root"
    );

    let response = client
        .get(format!("http://{addr}/api/traces/{}", traces[0].trace_id))
        .send()
        .await
        .expect("GET /api/traces/{id}");
    assert_eq!(response.status(), 200);
    let detail: saci_inspector_wire::TraceDetail = response.json().await.expect("detail decodes");
    assert!(!detail.spans.is_empty());

    let response = client
        .get(format!("http://{addr}/api/traces/999999999"))
        .send()
        .await
        .expect("GET unknown trace");
    assert_eq!(response.status(), 404, "an aged-out trace is a 404");

    let response = client
        .get(format!("http://{addr}/api/logs?limit=50&level=info"))
        .send()
        .await
        .expect("GET /api/logs");
    assert_eq!(response.status(), 200);
    let logs: Vec<saci_inspector_wire::LogRecord> = response.json().await.expect("logs decode");
    assert!(
        logs.iter().any(|log| log.message.contains("fixture run")),
        "the emitted event must be readable back: {logs:?}"
    );

    // ── /ui ──────────────────────────────────────────────────────────────────
    let response = client
        .get(format!("http://{addr}/ui"))
        .send()
        .await
        .expect("GET /ui");
    assert_eq!(response.status(), 200);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("text/html; charset=utf-8")
    );
    let html = response.text().await.expect("ui body");
    assert!(html.contains(r#"id="saci-app""#), "got: {html}");

    let response = client
        .get(format!("http://{addr}/ui/app_bg.wasm"))
        .send()
        .await
        .expect("GET /ui/app_bg.wasm");
    assert_eq!(response.status(), 200);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("application/wasm"),
        "instantiateStreaming rejects any other content type"
    );

    cancel.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn disabled_inspector_has_no_routes_at_all() {
    let (addr, cancel) = serve(state_with(None)).await;
    let client = reqwest::Client::new();

    for path in ["/api/topology", "/api/snapshot", "/api/logs", "/ui"] {
        let response = client
            .get(format!("http://{addr}{path}"))
            .send()
            .await
            .unwrap_or_else(|e| panic!("GET {path}: {e}"));
        assert_eq!(
            response.status(),
            404,
            "{path} must not exist when the inspector is off"
        );
    }

    let response = client
        .get(format!("http://{addr}/health"))
        .send()
        .await
        .expect("GET /health");
    assert_eq!(
        response.status(),
        200,
        "the control plane is unaffected by a disabled inspector"
    );

    cancel.cancel();
}
