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
//!   [`ServiceConfig`] schema, which is the same schema in every
//!   build: it can describe a cluster config, a `wasm` node or a `plugin`
//!   node whichever hosts are compiled in, and
//!   [`validate_build_capabilities`] is what refuses one this
//!   binary cannot run, naming the feature that would.
//! - `service-cluster`: adds the cluster runner ([`run_cluster`]) and
//!   enables `distributed-raft`, compiling in the openraft node, its redb log
//!   store, the replicated application state machine, and the TCP transport.
//!   Cluster state lives under `node.data_dir`, so it needs no other store.
//!
//! ## Quick start
//!
//! 1. Build your [`Pipeline`](crate::Pipeline) or another `PipelineRuntime`.
//! 2. Create a [`ServiceBuilder`] (`new` registers every compiled-in connector and codec) and call `runtime(...)`.
//! 3. Load a [`ServiceConfig`] from KDL.
//! 4. Call [`ServiceBuilder::build`] to get one [`BuiltWorkflow`] per
//!    declared workflow.

#[cfg(feature = "service")]
mod alert;
#[cfg(feature = "service")]
mod backfill;
#[cfg(feature = "service")]
mod builder;
#[cfg(feature = "service")]
pub mod cli;
#[cfg(feature = "service")]
mod operator_builder;

/// Install the workspace's `ring` crypto provider for reqwest's rustls.
///
/// reqwest 0.13's `rustls-no-provider` feature links rustls without a crypto
/// provider and refuses to build a `Client` until one is installed. The
/// workspace standardises on `ring` (the TLS backend the `postgresql` connector
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
mod cluster;
#[cfg(feature = "service")]
mod config;
#[cfg(all(
    feature = "service",
    any(feature = "wasm-host", feature = "plugin-host")
))]
pub(crate) mod digest;
#[cfg(feature = "service")]
mod dlq;
#[cfg(feature = "service")]
mod factories;
#[cfg(feature = "service")]
mod flow;
#[cfg(feature = "service")]
mod heal;
#[cfg(feature = "service")]
mod http;
#[cfg(feature = "service")]
mod lifecycle;
#[cfg(feature = "service")]
pub mod lineage;
#[cfg(all(feature = "service", feature = "wasm-host"))]
mod loader;
#[cfg(feature = "service")]
mod logging;
#[cfg(all(feature = "service", feature = "plugin-host"))]
mod plugin_loader;
#[cfg(feature = "service")]
mod redb_state;
#[cfg(feature = "service")]
mod registry;
#[cfg(feature = "service")]
mod runner;
#[cfg(feature = "service")]
mod sampling;
#[cfg(feature = "service")]
mod serve;
#[cfg(feature = "service")]
mod shutdown;
#[cfg(feature = "service")]
mod span_metrics;
#[cfg(feature = "service")]
mod standalone;
#[cfg(feature = "service")]
mod stream;
#[cfg(feature = "service")]
mod topology;
#[cfg(feature = "service")]
mod validation;
#[cfg(all(feature = "service", feature = "windows"))]
mod windowing;

#[cfg(feature = "service")]
pub use crate::connector::{ChannelBridge, ConnectorContext};
#[cfg(feature = "service")]
pub use alert::{
    AlertDispatcher, AlertEvent, AlertEventKind, AlertsConfig, PagerDutyTargetSpec,
    SlackTargetSpec, WebhookTargetSpec,
};
#[cfg(feature = "service")]
pub use builder::{
    BuiltEdge, BuiltNode, BuiltNodeKind, BuiltWorkflow, BuiltWorkflowBuilder, RunOptions,
    ServiceBuilder, WorkflowFactory, rebuild_blocker,
};
#[cfg(feature = "service-cluster")]
pub use cluster::{ClusterStats, run_cluster};
#[cfg(feature = "service")]
pub use config::{
    ClusterConfig, CodecEncryptionConfig, CodecSpec, CorruptRecordPolicy, CronSchedule,
    CronTimezone, DlqBlock, DlqConfig, DlqReplayPoint, FlowControlConfig, HealConfig, HttpConfig,
    KeySpec, LinkSpec, LogFormat, NodeConfig, NodeCryptoConfig, ObservabilityConfig, PeerSpec,
    PluginSpec, RetryConfig, RunMode, SchemaEvolutionConfig, SchemaSpec, ServiceConfig,
    ServiceEncryptionConfig, ServiceMode, SinkSpec, SourceSpec, StandaloneConfig, StoreConfig,
    WasmSpec, WindowConfig, WorkflowSpec,
};
#[cfg(feature = "service")]
pub use dlq::{DeadLetterQueue, DlqError, DlqRegistry, DlqShared, ReplayFilter};
#[cfg(feature = "service")]
pub use factories::{
    BUILTIN_CODEC_FEATURES, BUILTIN_CONNECTOR_FEATURES, builtin_codec_feature, builtin_feature,
    missing_cluster_host_error,
};
#[cfg(feature = "service")]
pub use flow::{
    FlowAdjustment, FlowCause, FlowController, FlowMove, FlowOutcome, FlowPlan, FlowSample,
    FlowSettings,
};
#[cfg(feature = "service")]
pub use heal::{HealSettings, HealingSink, HealingSource};
#[cfg(feature = "service")]
pub use http::{ServiceModeLabel, ServiceState, build_router, serve_http, spawn_watchdog};
#[cfg(feature = "service")]
pub use lifecycle::{
    LIFECYCLE_ACK_BUDGET, LifecycleError, LifecycleRegistry, LifecycleRegistryBuilder, PauseGate,
    PauseHandle, RunControl, SupervisorChannels, WorkflowCommand, run_supervised,
};
#[cfg(feature = "service")]
pub use lineage::{
    LineageConfig, LineageDispatcher, LineageEmitMode, LineageEventType, LineageSnapshot,
    OpenLineageEvent, WorkflowLineageGraph, build_service_lineage, build_workflow_lineage,
};
#[cfg(all(feature = "service", feature = "wasm-host"))]
pub use loader::{LocalModuleResolver, ModuleResolver, PipelineRuntimeLoader};
#[cfg(feature = "service")]
pub use logging::{TelemetryGuard, init_logging};
#[cfg(all(feature = "service", feature = "plugin-host"))]
pub use plugin_loader::load_plugin_runtime;
#[cfg(feature = "service")]
pub use redb_state::{RedbStateClient, SourceCursorMeta};
#[cfg(feature = "service")]
pub use registry::{Codec, CodecFactory, CodecRegistry, Connector, Rebuild, Registry, Role};
#[cfg(feature = "service")]
pub use serve::{ServeExit, ServeOptions, serve};
#[cfg(feature = "service")]
pub use shutdown::ShutdownCoordinator;
#[cfg(feature = "service")]
pub use span_metrics::SpanMetricsLayer;
#[cfg(feature = "service")]
pub use standalone::{NodeStats, WorkflowStats};
#[cfg(feature = "service")]
pub use topology::build_topology;
#[cfg(feature = "service")]
pub use validation::{
    validate_build_capabilities, validate_schema_fingerprint, validate_workflow_graph,
};
