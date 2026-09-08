//! # Service Runner
//!
//! Top-level configuration schema, factory registry, service builder, and the
//! standalone and cluster runners.
//!
//! ## Feature layering
//!
//! The service layer is split across two features so operators who only need
//! standalone mode do not pay for the Raft stack:
//!
//! - `service`: standalone-only control plane. HTTP API, KDL config, logging,
//!   Prometheus, the `saci-service` binary, and the core
//!   [`config::ServiceConfig`] schema, which is the same schema in every
//!   build: it can describe a cluster config, a `wasm` node or a `plugin`
//!   node whichever hosts are compiled in, and
//!   [`validation::validate_build_capabilities`] is what refuses one this
//!   binary cannot run, naming the feature that would.
//! - `service-cluster`: adds the cluster runner ([`cluster::run_cluster`]) and
//!   enables `distributed-raft`, compiling in the openraft node, its redb log
//!   store, the replicated application state machine, and the TCP transport.
//!   Cluster state lives under `node.data_dir`, so it needs no other store.
//!
//! ## Quick start
//!
//! 1. Build your [`Pipeline`](saci_core::Pipeline) or `Box<dyn PipelineRuntime>`.
//! 2. Create a [`builder::ServiceBuilder`], call `with_runtime(...)`, and register IO factories.
//! 3. Load a [`config::ServiceConfig`] from KDL.
//! 4. Call [`builder::ServiceBuilder::build_all`] to get one [`builder::BuiltService`] per
//!    declared workflow.

#[cfg(feature = "service")]
pub mod builder;

/// Install the workspace's `ring` crypto provider for reqwest's rustls.
///
/// reqwest 0.13's `rustls-no-provider` feature links rustls without a crypto
/// provider and refuses to build a `Client` until one is installed. The
/// workspace standardises on `ring` (the TLS backend saci-connector-postgresql
/// links), so every reqwest user (the CLI's `status`/`cluster` commands and
/// the HTTP test clients) calls this before constructing a client.
/// Idempotent and process-global: a provider another caller installed, or a
/// racing install from another thread, is left in place.
#[cfg(feature = "service")]
pub fn install_ring_provider() {
    use std::sync::OnceLock;

    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}
#[cfg(feature = "service-cluster")]
pub mod cluster;
#[cfg(feature = "service")]
pub mod config;
#[cfg(all(feature = "service", any(feature = "wasm", feature = "plugin")))]
pub(crate) mod digest;
#[cfg(feature = "service")]
pub mod dlq;
#[cfg(feature = "service")]
pub mod dlq_api;
#[cfg(feature = "service")]
pub mod factories;
#[cfg(feature = "service")]
pub mod flow;
#[cfg(feature = "service")]
pub mod heal;
#[cfg(feature = "service")]
pub mod http;
#[cfg(feature = "service")]
pub mod inspector_api;
#[cfg(feature = "service")]
pub mod lifecycle;
#[cfg(feature = "service")]
pub mod lifecycle_api;
#[cfg(all(feature = "service", feature = "wasm"))]
pub mod loader;
#[cfg(feature = "service")]
pub mod logging;
#[cfg(all(feature = "service", feature = "plugin"))]
pub mod plugin_loader;
#[cfg(feature = "service")]
pub mod redb_state;
#[cfg(feature = "service")]
pub mod registry;
#[cfg(feature = "service")]
pub mod sampling;
#[cfg(feature = "service")]
pub mod shutdown;
#[cfg(feature = "service")]
pub mod span_metrics;
#[cfg(feature = "service")]
pub mod standalone;
#[cfg(feature = "service")]
pub mod stream;
#[cfg(feature = "service")]
pub mod topology;
#[cfg(feature = "service")]
pub mod validation;
#[cfg(all(feature = "service", feature = "windows"))]
pub mod windowing;

#[cfg(feature = "service")]
pub use builder::{BuiltNode, BuiltNodeKind, BuiltService, ServiceBuilder};
#[cfg(feature = "service-cluster")]
pub use cluster::{ClusterStats, run_cluster};
#[cfg(feature = "service")]
pub use config::{
    ClusterConfig, DlqBlock, DlqConfig, DlqReplayPoint, FlowControlConfig, HealConfig, HttpConfig,
    LinkSpec, LogFormat, NodeConfig, ObservabilityConfig, PeerSpec, PluginSpec, RunMode,
    ServiceConfig, ServiceMode, SinkSpec, SourceSpec, StandaloneConfig, TransformerSpec, WasmSpec,
    WorkflowSpec,
};
#[cfg(feature = "service")]
pub use dlq::{DeadLetterQueue, DlqError, DlqRegistry, DlqShared, ReplayFilter};
#[cfg(feature = "service")]
pub use factories::register_builtin_factories;
#[cfg(feature = "service")]
pub use flow::{
    FlowAdjustment, FlowCause, FlowController, FlowMove, FlowOutcome, FlowPlan, FlowSample,
    FlowSettings,
};
#[cfg(feature = "service")]
pub use heal::{HealSettings, HealingSink, HealingSource};
#[cfg(feature = "service")]
pub use http::{
    ClusterProbe, ClusterProbeSnapshot, ServiceModeLabel, ServiceState, build_router, serve_http,
    spawn_watchdog,
};
#[cfg(feature = "service")]
pub use lifecycle::{
    LIFECYCLE_ACK_BUDGET, LifecycleError, LifecycleRegistry, LifecycleRegistryBuilder, PauseGate,
    PauseHandle, RunControl, SupervisorChannels, WorkflowCommand, run_supervised,
};
#[cfg(all(feature = "service", feature = "wasm"))]
pub use loader::{LocalModuleResolver, ModuleResolver, PipelineRuntimeLoader};
#[cfg(feature = "service")]
pub use logging::{TelemetryGuard, init_logging};
#[cfg(all(feature = "service", feature = "plugin"))]
pub use plugin_loader::load_plugin_runtime;
#[cfg(feature = "service")]
pub use redb_state::{RedbStateClient, SourceCursorMeta};
#[cfg(feature = "service")]
pub use registry::{
    Registry, SinkFactory, SourceFactory, Transformer, TransformerFactory, TransformerRegistry,
};
#[cfg(feature = "service")]
pub use saci_connector::{ChannelBridge, ConnectorContext};
#[cfg(feature = "service")]
pub use shutdown::ShutdownCoordinator;
#[cfg(feature = "service")]
pub use span_metrics::SpanMetricsLayer;
#[cfg(feature = "service")]
pub use standalone::{StandaloneStats, run_standalone};
#[cfg(feature = "service")]
pub use stream::run_stream;
#[cfg(feature = "service")]
pub use topology::build_topology;
#[cfg(feature = "service")]
pub use validation::{validate_build_capabilities, validate_workflow_graph};
