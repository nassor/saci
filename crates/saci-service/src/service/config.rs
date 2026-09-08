//! # Service Configuration Schema
//!
//! KDL configuration schema for the SACI service runner. Operators write a
//! single file; the service parses, validates, and boots from it.
//!
//! ## Example (standalone)
//!
//! ```kdl
//! mode "standalone"
//!
//! node id=1 data_dir="/var/lib/saci"
//!
//! run_mode kind="interval" interval_ms=5000
//!
//! workflow "etl" name="ETL" {
//!     wasm "transform" module="pipelines/transform.wasm" {
//!         config batch_size="1000"
//!     }
//! }
//! ```
//!
//! One `workflow` declares the whole DAG: sources, `wasm`/`plugin` processors,
//! sinks and transformers, each with a mandatory id and an optional name,
//! connected by explicit `link from="..." to="..."` declarations. See
//! [`WorkflowSpec`].
//!
//! Cluster mode instead sets `mode "cluster"`, `bootstrap` on the first node,
//! and one `peer` node (`id`, `addr`) per cluster member, and takes no
//! `store` block: cluster state is the raft-replicated `cluster-app.redb`
//! under `node.data_dir`. A cluster-mode workflow declares exactly one
//! `wasm` or `plugin` node and no source, sink or link: the distributed
//! runner ingests through `PartitionSource` and drives one runtime per node.
//!
//! ## Env var substitution
//!
//! Any `${VAR}` placeholder in the file is replaced with the matching env var.
//! `${VAR:-default}` falls back to `default` if `VAR` is unset. Substitution
//! runs over the raw text before the parser, so a template stays valid KDL
//! either side of it.
//!
//! A top-level `variables { name "value" }` block declares names that win
//! over a same-named process env var, with the environment still the
//! fallback for undeclared names, so a file can reference its own
//! declarations with the same `${name}` / `${name:-default}` syntax.
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use saci_config::one_or_many;
use serde::{Deserialize, Serialize};

use crate::error::{SaciError, SaciResult};
use saci_core::retry::{RetryMode, SystemConfig};

use super::flow::FlowSettings;
use super::heal::HealSettings;

/// The configuration language, re-exported so an embedder that reads
/// [`SourceSpec::config`] or writes a factory names one path.
pub use saci_config::{
    ConfigMap, ConfigValue, from_kdl_str, from_kdl_str_with_vars, substitute_env_vars,
    substitute_vars,
};

/// Default opaque per-instance config: an empty table.
///
/// Serde's own `Default` for the value type is null, which every factory
/// rejects.
fn default_config() -> ConfigValue {
    ConfigValue::Object(ConfigMap::new())
}
/// Default total attempts (initial + retries) for a source or sink node.
fn default_retry_attempts() -> u32 {
    4
}
/// Default delay before the second attempt.
fn default_retry_base_ms() -> u64 {
    100
}
/// Default growth factor applied per attempt.
fn default_retry_mult() -> f64 {
    2.0
}
/// Default ceiling on the computed delay.
fn default_retry_max_ms() -> u64 {
    30_000
}
/// Default fraction of the delay randomised.
fn default_retry_jitter() -> f64 {
    0.1
}

// Flexible deserializers: accept either the native type, or a string that
// parses to it. Lets env-var substitution work for non-string fields
// (`id="${SACI_NODE_ID}"`).

fn de_u64_flexible<'de, D>(d: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum U64OrStr {
        Int(u64),
        Str(String),
    }
    match U64OrStr::deserialize(d)? {
        U64OrStr::Int(n) => Ok(n),
        U64OrStr::Str(s) => s.trim().parse().map_err(serde::de::Error::custom),
    }
}

fn de_bool_flexible<'de, D>(d: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum BoolOrStr {
        Bool(bool),
        Str(String),
    }
    match BoolOrStr::deserialize(d)? {
        BoolOrStr::Bool(b) => Ok(b),
        BoolOrStr::Str(s) => s.trim().parse().map_err(serde::de::Error::custom),
    }
}

fn de_opt_u64_flexible<'de, D>(d: D) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum U64OrStr {
        Int(u64),
        Str(String),
    }
    match Option::<U64OrStr>::deserialize(d)? {
        None => Ok(None),
        Some(U64OrStr::Int(n)) => Ok(Some(n)),
        Some(U64OrStr::Str(s)) => s.trim().parse().map(Some).map_err(serde::de::Error::custom),
    }
}

fn de_opt_bool_flexible<'de, D>(d: D) -> Result<Option<bool>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum BoolOrStr {
        Bool(bool),
        Str(String),
    }
    match Option::<BoolOrStr>::deserialize(d)? {
        None => Ok(None),
        Some(BoolOrStr::Bool(b)) => Ok(Some(b)),
        Some(BoolOrStr::Str(s)) => s.trim().parse().map(Some).map_err(serde::de::Error::custom),
    }
}

fn de_opt_f64_flexible<'de, D>(d: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum F64OrStr {
        Num(f64),
        Str(String),
    }
    match Option::<F64OrStr>::deserialize(d)? {
        None => Ok(None),
        Some(F64OrStr::Num(n)) => Ok(Some(n)),
        Some(F64OrStr::Str(s)) => s.trim().parse().map(Some).map_err(serde::de::Error::custom),
    }
}

/// Identity and storage configuration for a single service node.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeConfig {
    /// Raft node ID: unique in the cluster and stable across restarts.
    ///
    /// Accepts either a bare integer or a string parseable as `u64`. The
    /// string form lets env-var substitution work in templates
    /// (`id="${SACI_NODE_ID}"` stays valid KDL before substitution).
    #[serde(deserialize_with = "de_u64_flexible")]
    pub id: u64,
    /// Human-readable label used in logs and metrics. Optional.
    #[serde(default)]
    pub name: Option<String>,
    /// Filesystem path used for redb data files and WAL.
    pub data_dir: PathBuf,
}

impl NodeConfig {
    /// The service label: `name` when set, otherwise the decimal `id`.
    ///
    /// A `saci` sink announces this to its peer, and the `saci` source on the
    /// other end labels its own series with it as `peer_service`.
    pub fn label(&self) -> String {
        self.name.clone().unwrap_or_else(|| self.id.to_string())
    }
}

/// How the standalone service drives pipeline execution.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RunMode {
    /// Run continuously, re-entering the pipeline as the source produces work.
    ///
    /// A pass that drained every source (or admitted nothing) waits 100 ms
    /// before the next; one that spent its flow-control credit with the source
    /// still live, or left a carry-over slice, re-enters immediately.
    #[default]
    Continuous,
    /// Run the pipeline exactly once, then exit.
    OneShot,
    /// Re-run the pipeline every `interval_ms` milliseconds while its sources
    /// are drained.
    Interval {
        /// Milliseconds between successive pipeline runs.
        ///
        /// The idle poll cadence, not a throughput cap: an iteration that
        /// spent its flow-control credit with the source still live, or that
        /// left a carry-over slice, re-enters immediately and this wait is
        /// skipped. Only a pass that drained every source pays it.
        interval_ms: u64,
    },
    /// Process each source batch as it arrives, in flow-control-sized chunks
    /// (streaming).
    ///
    /// Requires at least one declared source and standalone mode. Sources are
    /// pulled round-robin, and each arriving batch is sliced at that source's
    /// current flow-control target (zero-copy), so one arrival can be several
    /// items. A source on a path to a windowed node carries no controller here
    /// and is never sliced: one arrival is one item, sized by the connector.
    /// No inter-item sleep: latency is bounded by the pipeline itself.
    Stream,
}

/// Configuration for standalone (single-node) mode.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct StandaloneConfig {
    /// Determines how the service drives pipeline runs.
    #[serde(default)]
    pub run_mode: RunMode,
}

/// A peer in the Raft cluster.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PeerSpec {
    /// Raft node ID of this peer.
    pub id: u64,
    /// Network address in `host:port` format.
    pub addr: String,
}

fn default_election_timeout() -> u64 {
    1_500
}
fn default_heartbeat_interval() -> u64 {
    300
}
fn default_snapshot_log_interval() -> u64 {
    10_000
}
fn default_lease_ttl() -> u64 {
    30_000
}

/// Configuration for cluster (multi-node Raft) mode.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClusterConfig {
    /// All peers in the cluster, including this node. One `peer` node each.
    #[serde(rename = "peer", deserialize_with = "one_or_many")]
    pub peers: Vec<PeerSpec>,
    /// Bootstrap a fresh cluster when `data_dir` is empty.
    /// Set to `false` for nodes that join an existing cluster.
    ///
    /// Accepts either a bare bool or a string parseable as `bool`
    /// (`"true"` / `"false"`). The string form lets env-var substitution work
    /// in templates.
    #[serde(default, deserialize_with = "de_bool_flexible")]
    pub bootstrap: bool,
    /// How long a claimed row-range lease is held before it may be reclaimed.
    ///
    /// Must be at least three election timeouts: a batch lease has to outlive
    /// one SACI election, or a leader change alone would let a second node
    /// claim a range that is still being processed.
    #[serde(default = "default_lease_ttl")]
    pub lease_ttl_ms: u64,
    /// Raft election timeout in milliseconds.
    #[serde(default = "default_election_timeout")]
    pub election_timeout_ms: u64,
    /// Raft heartbeat interval in milliseconds.
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_ms: u64,
    /// Take a snapshot, and purge the log behind it, every N committed log
    /// entries. Sets the Raft snapshot policy; must be at least 1, because a
    /// zero interval would snapshot on every entry.
    #[serde(default = "default_snapshot_log_interval")]
    pub snapshot_log_interval: u64,
}

/// Which runtime mode the service runs in.
///
/// The `mode` tag sits at the top of the document:
///
/// ```kdl
/// mode "standalone"
/// // or
/// mode "cluster"
/// // plus one `peer` node per cluster member
/// ```
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum ServiceMode {
    /// Single-node operation. No consensus required.
    Standalone {
        /// Standalone-specific options flattened into the top-level document.
        #[serde(flatten, default)]
        config: StandaloneConfig,
    },
    /// Multi-node Raft cluster.
    Cluster {
        /// Cluster-specific options flattened into the top-level document.
        #[serde(flatten)]
        config: ClusterConfig,
    },
}

/// Persistent store declaration, tagged by `store`.
///
/// ```kdl
/// store "redb" {
///     path "/var/lib/saci/state.redb"
///     batch_resume #true
/// }
/// ```
///
/// With a `store` block present, the pipeline config is persisted to the
/// store before the pipeline builds and stream-mode resume state (processor
/// priors and source cursors) is written back as work progresses.
/// Interval/one-shot batch state carry is opt-in per store via
/// `batch_resume`.
///
/// This is standalone-mode persistence only: the file is local and
/// unreplicated. Cluster mode takes no `store` block, because its
/// application state is the raft-replicated `cluster-app.redb` under
/// `node.data_dir`.
#[derive(Debug, Clone, Serialize)]
pub enum StoreConfig {
    /// A local embedded redb file.
    Redb {
        /// Path to the redb file. Created on first use.
        path: PathBuf,
        /// Opt-in interval/one-shot processor state carry across iterations.
        batch_resume: bool,
    },
}

impl<'de> Deserialize<'de> for StoreConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // The KDL parser stores a node's leading argument under `id` (see
        // saci-config rules), so the tag arrives as an `id` entry rather than
        // a `store` key. Read it first, then validate the remaining keys
        // against the variant's shape.
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RedbInner {
            id: String,
            path: PathBuf,
            #[serde(default)]
            batch_resume: bool,
        }

        let inner = RedbInner::deserialize(deserializer)?;
        if inner.id != "redb" {
            return Err(serde::de::Error::custom(format!(
                "unknown store kind '{}' (expected \"redb\")",
                inner.id
            )));
        }
        Ok(StoreConfig::Redb {
            path: inner.path,
            batch_resume: inner.batch_resume,
        })
    }
}

/// A windowing declaration on a processor node: which windows the host
/// assumes the processor's merging logic works in, and what it tracks for
/// them.
///
/// The geometry lives on the node itself, tagged by `kind`, because a KDL
/// property table is a flat map and the spec's own `kind` tag is one key of
/// it. Key fields are child nodes: a KDL property cannot repeat, and a child
/// with one argument is a scalar while one with several is an array, which is
/// exactly the one-or-many shape a key list needs.
///
/// ```kdl
/// wasm "windowed" module="pipelines/windowed.wasm" {
///     window kind="tumbling" size_ms=30000 offset_ms=0
///            time_field="timestamp_ms" allowed_lateness_ms=5000 {
///         key_field "category"
///     }
/// }
/// ```
///
/// The host uses the declaration to validate the node's inbound streams
/// (every delivered component must carry `time_field`), to track the
/// node's event-time watermark across batches, and to describe the node in
/// the dashboard. The processor itself implements the merging logic and
/// reads the same geometry back through `get-config` under the `window.*`
/// keys, which the builder injects into the node's config table; the two
/// sides of the contract cannot drift because there is only one source of
/// truth.
///
/// Declared in every build, like the `wasm` and `plugin` node kinds that
/// carry it. The geometry is
/// [`saci_core::window_spec::WindowSpec`], which sits outside saci-core's
/// `windows` feature for exactly this reason, so a binary built without the
/// windowing engine still parses the block and
/// [`validate_build_capabilities`](super::validation::validate_build_capabilities)
/// names the `--features windows` flag that would serve it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WindowConfig {
    /// The window geometry (tumbling, sliding or session).
    #[serde(flatten)]
    pub spec: saci_core::window_spec::WindowSpec,
    /// The event-time column every component delivered to this processor
    /// must carry. Supports `Int64` milliseconds and the Arrow timestamp
    /// types.
    pub time_field: String,
    /// Grouping key columns; empty for a global window.
    #[serde(rename = "key_field")]
    pub key_fields: Vec<String>,
    /// How many milliseconds past the watermark a late row is still accepted
    /// by the processor's windowing logic. Defaults to 0.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub allowed_lateness_ms: i64,
}

fn is_zero(value: &i64) -> bool {
    *value == 0
}

impl WindowConfig {
    /// Whether the declaration is geometrically sane and names a non-empty
    /// time field with a non-negative lateness budget.
    pub fn validate(&self) -> Result<(), String> {
        self.spec.validate()?;
        if self.time_field.is_empty() {
            return Err("window time_field must not be empty".to_string());
        }
        if self.allowed_lateness_ms < 0 {
            return Err(format!(
                "window allowed_lateness_ms must be >= 0, got {}",
                self.allowed_lateness_ms
            ));
        }
        Ok(())
    }

    /// The declaration as `window.*` config keys, for injection into the
    /// processor node's `config` table so `get-config` answers them.
    pub fn config_pairs(&self) -> Vec<(String, String)> {
        let mut pairs = Vec::with_capacity(6);
        let (kind, size, slide, offset, gap) = match &self.spec {
            saci_core::window_spec::WindowSpec::Tumbling { size_ms, offset_ms } => {
                ("tumbling", Some(*size_ms), None, Some(*offset_ms), None)
            }
            saci_core::window_spec::WindowSpec::Sliding {
                size_ms,
                slide_ms,
                offset_ms,
            } => (
                "sliding",
                Some(*size_ms),
                Some(*slide_ms),
                Some(*offset_ms),
                None,
            ),
            saci_core::window_spec::WindowSpec::Session { gap_ms } => {
                ("session", None, None, None, Some(*gap_ms))
            }
        };
        pairs.push(("window.kind".to_string(), kind.to_string()));
        if let Some(size) = size {
            pairs.push(("window.size_ms".to_string(), size.to_string()));
        }
        if let Some(slide) = slide {
            pairs.push(("window.slide_ms".to_string(), slide.to_string()));
        }
        if let Some(offset) = offset {
            pairs.push(("window.offset_ms".to_string(), offset.to_string()));
        }
        if let Some(gap) = gap {
            pairs.push(("window.gap_ms".to_string(), gap.to_string()));
        }
        pairs.push(("window.time_field".to_string(), self.time_field.clone()));
        pairs.push(("window.key_fields".to_string(), self.key_fields.join(",")));
        pairs.push((
            "window.allowed_lateness_ms".to_string(),
            self.allowed_lateness_ms.to_string(),
        ));
        pairs
    }
}

impl<'de> Deserialize<'de> for WindowConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{Error as _, MapAccess, Visitor};

        struct WindowConfigVisitor;

        impl<'de> Visitor<'de> for WindowConfigVisitor {
            type Value = WindowConfig;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(
                    "a window node: kind, geometry, time_field, optional key_field(s) and \
                     allowed_lateness_ms",
                )
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                const KNOWN: &[&str] = &[
                    "kind",
                    "size_ms",
                    "slide_ms",
                    "offset_ms",
                    "gap_ms",
                    "time_field",
                    "allowed_lateness_ms",
                    "key_field",
                ];
                let mut kind: Option<String> = None;
                let mut size_ms: Option<i64> = None;
                let mut slide_ms: Option<i64> = None;
                let mut offset_ms: Option<i64> = None;
                let mut gap_ms: Option<i64> = None;
                let mut time_field: Option<String> = None;
                let mut allowed_lateness_ms: Option<i64> = None;
                let mut key_fields: Vec<String> = Vec::new();

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "kind" => kind = Some(map.next_value()?),
                        "size_ms" => size_ms = Some(map.next_value()?),
                        "slide_ms" => slide_ms = Some(map.next_value()?),
                        "offset_ms" => offset_ms = Some(map.next_value()?),
                        "gap_ms" => gap_ms = Some(map.next_value()?),
                        "time_field" => time_field = Some(map.next_value()?),
                        "allowed_lateness_ms" => allowed_lateness_ms = Some(map.next_value()?),
                        "key_field" => {
                            #[derive(Deserialize)]
                            #[serde(untagged)]
                            enum OneOrMany {
                                One(String),
                                Many(Vec<String>),
                            }
                            key_fields = match map.next_value::<OneOrMany>()? {
                                OneOrMany::One(one) => vec![one],
                                OneOrMany::Many(many) => many,
                            };
                        }
                        other => return Err(A::Error::unknown_field(other, KNOWN)),
                    }
                }

                let kind = kind.ok_or_else(|| A::Error::missing_field("kind"))?;
                let spec = match kind.as_str() {
                    "tumbling" => {
                        if slide_ms.is_some() {
                            return Err(A::Error::custom(
                                "slide_ms is only valid on a sliding window",
                            ));
                        }
                        if gap_ms.is_some() {
                            return Err(A::Error::custom(
                                "gap_ms is only valid on a session window",
                            ));
                        }
                        saci_core::window_spec::WindowSpec::Tumbling {
                            size_ms: size_ms.ok_or_else(|| A::Error::missing_field("size_ms"))?,
                            offset_ms: offset_ms.unwrap_or(0),
                        }
                    }
                    "sliding" => {
                        if gap_ms.is_some() {
                            return Err(A::Error::custom(
                                "gap_ms is only valid on a session window",
                            ));
                        }
                        saci_core::window_spec::WindowSpec::Sliding {
                            size_ms: size_ms.ok_or_else(|| A::Error::missing_field("size_ms"))?,
                            slide_ms: slide_ms
                                .ok_or_else(|| A::Error::missing_field("slide_ms"))?,
                            offset_ms: offset_ms.unwrap_or(0),
                        }
                    }
                    "session" => {
                        if size_ms.is_some() {
                            return Err(A::Error::custom(
                                "size_ms is only valid on a tumbling or sliding window",
                            ));
                        }
                        if slide_ms.is_some() {
                            return Err(A::Error::custom(
                                "slide_ms is only valid on a sliding window",
                            ));
                        }
                        if offset_ms.is_some() {
                            return Err(A::Error::custom(
                                "offset_ms is only valid on a tumbling or sliding window",
                            ));
                        }
                        saci_core::window_spec::WindowSpec::Session {
                            gap_ms: gap_ms.ok_or_else(|| A::Error::missing_field("gap_ms"))?,
                        }
                    }
                    other => {
                        return Err(A::Error::unknown_variant(
                            other,
                            &["tumbling", "sliding", "session"],
                        ));
                    }
                };

                Ok(WindowConfig {
                    spec,
                    time_field: time_field.ok_or_else(|| A::Error::missing_field("time_field"))?,
                    key_fields,
                    allowed_lateness_ms: allowed_lateness_ms.unwrap_or(0),
                })
            }
        }

        deserializer.deserialize_map(WindowConfigVisitor)
    }
}

/// WASM processor node (requires the `wasm` feature at runtime).
///
/// ```kdl
/// workflow "w" {
///     wasm "transform" module="pipelines/transform.wasm" sha3_256="abc123..." {
///         config batch_size="1000"
///     }
/// }
/// ```
///
/// `module` is optional: a node declared with no `module` key is a processor
/// whose runtime is supplied programmatically through
/// [`ServiceBuilder::with_runtime`](crate::service::builder::ServiceBuilder::with_runtime),
/// looked up by this node's `id`. Building a workflow where such a node has
/// neither a `module` nor a matching registered runtime is a build-time
/// error.
///
/// Unknown keys are rejected: a key the service cannot honour is a
/// configuration error, not something to ignore silently.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct WasmSpec {
    /// Mandatory id, from the node's leading argument. Unique workflow-wide.
    pub id: String,
    /// Optional display name. The dashboard shows the id when this is absent.
    #[serde(default)]
    pub name: Option<String>,
    /// Path to the `.wasm` component file (relative or absolute). Absent for
    /// a node whose runtime is supplied through `with_runtime`.
    #[serde(default)]
    pub module: Option<String>,
    /// Optional expected SHA3-256 hex digest of the module bytes. Validation
    /// fails at load time if the digest does not match. The value may carry
    /// an optional `sha3-256:` prefix.
    #[serde(default)]
    pub sha3_256: Option<String>,
    /// Opaque key-value config the processor reads through the
    /// `saci:pipeline/host-io` `get-config` import.
    #[serde(default)]
    pub config: HashMap<String, String>,
    /// Windowing declaration: how the host merges this node's inbound
    /// streams and tracks its event-time watermark. The geometry is injected
    /// into `config` as `window.*` keys, so the processor's merging logic
    /// reads one source of truth.
    #[serde(default)]
    pub window: Option<WindowConfig>,
}

/// Native plugin processor node (requires the `plugin` feature at runtime).
///
/// ```kdl
/// workflow "w" {
///     plugin "audit" library="pipelines/libtransform.so" sha3_256="abc123..." {
///         config batch_size="1000"
///     }
/// }
/// ```
///
/// The key is `library`, not `module`, because the artifact is a shared
/// library the host loads with `dlopen`. It is optional for the same reason
/// [`WasmSpec::module`] is: a node with no `library` relies on a runtime
/// registered under this node's `id` through
/// [`ServiceBuilder::with_runtime`](crate::service::builder::ServiceBuilder::with_runtime).
///
/// Unknown keys are rejected: a key the service cannot honour is a
/// configuration error, not something to ignore silently.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct PluginSpec {
    /// Mandatory id, from the node's leading argument. Unique workflow-wide.
    pub id: String,
    /// Optional display name. The dashboard shows the id when this is absent.
    #[serde(default)]
    pub name: Option<String>,
    /// Path to the shared library file (relative or absolute). Absent for a
    /// node whose runtime is supplied through `with_runtime`.
    #[serde(default)]
    pub library: Option<String>,
    /// Optional expected SHA3-256 hex digest of the library file bytes.
    /// Validation fails at load time if the digest does not match. The value
    /// may carry an optional `sha3-256:` prefix.
    #[serde(default)]
    pub sha3_256: Option<String>,
    /// Opaque key-value config the plugin reads through the `get_config`
    /// callback on the host vtable.
    #[serde(default)]
    pub config: HashMap<String, String>,
    /// Windowing declaration: how the host merges this node's inbound
    /// streams and tracks its event-time watermark. The geometry is injected
    /// into `config` as `window.*` keys, so the plugin's merging logic reads
    /// one source of truth.
    #[serde(default)]
    pub window: Option<WindowConfig>,
}

/// A declared byte-format instance: a name a source or sink's `transformer`
/// key can reference.
///
/// ```kdl
/// workflow "w" {
///     transformer "orders-json" name="Orders NDJSON" format="ndjson" {
///         options infer_max=1000
///     }
/// }
/// ```
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct TransformerSpec {
    /// Mandatory id, from the node's leading argument. Unique workflow-wide.
    pub id: String,
    /// Optional display name. The dashboard shows the id when this is absent.
    #[serde(default)]
    pub name: Option<String>,
    /// Registered format name, resolved against the `TransformerRegistry`.
    pub format: String,
    /// Handed to that format's factory. Empty table when absent.
    #[serde(default = "default_config")]
    pub options: ConfigValue,
}

/// Retry policy for a source or sink node.
///
/// An omitted `retry` block uses the same policy as
/// `saci_core::retry::SystemConfig::default()`: 4 attempts (3 retries) with
/// 100 ms base delay, 2.0x multiplier, 30 s cap and 0.1 jitter.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RetryConfig {
    /// Total attempts including the initial one; 1 disables retrying.
    #[serde(default = "default_retry_attempts")]
    pub max_attempts: u32,
    /// Delay before the second attempt.
    #[serde(default = "default_retry_base_ms")]
    pub base_delay_ms: u64,
    /// Growth factor applied per attempt.
    #[serde(default = "default_retry_mult")]
    pub multiplier: f64,
    /// Ceiling on the computed delay.
    #[serde(default = "default_retry_max_ms")]
    pub max_delay_ms: u64,
    /// Fraction of the delay randomised, in `0.0..=1.0`.
    #[serde(default = "default_retry_jitter")]
    pub jitter: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: default_retry_attempts(),
            base_delay_ms: default_retry_base_ms(),
            multiplier: default_retry_mult(),
            max_delay_ms: default_retry_max_ms(),
            jitter: default_retry_jitter(),
        }
    }
}

impl RetryConfig {
    fn validate(&self, what: &str) -> Result<(), SaciError> {
        if self.max_attempts < 1 {
            return Err(SaciError::configuration(format!(
                "{what}: retry.max_attempts must be at least 1"
            )));
        }
        if self.multiplier < 1.0 || self.multiplier.is_nan() {
            return Err(SaciError::configuration(format!(
                "{what}: retry.multiplier must be at least 1.0, got {}",
                self.multiplier
            )));
        }
        if !(0.0..=1.0).contains(&self.jitter) {
            return Err(SaciError::configuration(format!(
                "{what}: retry.jitter must be within 0.0..=1.0, got {}",
                self.jitter
            )));
        }
        Ok(())
    }

    /// The `saci-core` retry policy this config drives.
    pub fn to_system_config(&self) -> SystemConfig {
        if self.max_attempts == 1 {
            SystemConfig::minimal()
        } else {
            SystemConfig {
                retry_mode: RetryMode::ExponentialBackoff {
                    max_retries: (self.max_attempts - 1) as usize,
                    base_delay: Duration::from_millis(self.base_delay_ms),
                    multiplier: self.multiplier,
                    max_delay: Duration::from_millis(self.max_delay_ms),
                    jitter: self.jitter,
                },
            }
        }
    }
}

/// Self-healing policy for a source or sink node: when the host stops
/// re-driving a failing connector and replaces it with a fresh one built
/// from the same factory and the same `config`.
///
/// On by default. Declared once at the top level, and overridden field by
/// field per node:
///
/// ```kdl
/// heal {
///     enabled #true
///     after_failures 3
///     base_delay_ms 1000
///     multiplier 2.0
///     max_delay_ms 60000
///     jitter 0.1
///     max_attempts 0
/// }
///
/// workflow "w" {
///     sink "collector" type="tcp" component="Order" {
///         heal { after_failures 1 }
///     }
/// }
/// ```
///
/// | Key | Unit | Default | Trade-off |
/// |---|---|---|---|
/// | `enabled` | flag | `#true` | `#false` keeps re-driving the same instance forever |
/// | `after_failures` | failures | 3 | consecutive failures before the first rebuild; higher rides out a longer blip |
/// | `base_delay_ms` | ms | 1000 | delay before the first rebuild |
/// | `multiplier` | ratio | 2.0 | growth per further attempt; must be at least 1.0 |
/// | `max_delay_ms` | ms | 60000 | ceiling on the computed delay |
/// | `jitter` | ratio | 0.1 | fraction of the delay randomised, in `0.0..=1.0` |
/// | `max_attempts` | attempts | 0 | rebuilds before the node gives up; `0` never gives up |
///
/// A connector answers [`SourceFactory::rebuildable`](saci_connector::SourceFactory::rebuildable)
/// for its own config, and one that cannot be rebuilt is never healed. A
/// node declaring its own `heal` block on such a connector is a load-time
/// error rather than a block that quietly does nothing.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct HealConfig {
    /// Whether a failing connector is replaced at all. Default `true`.
    #[serde(deserialize_with = "de_opt_bool_flexible")]
    pub enabled: Option<bool>,
    /// Consecutive failed operations before the first rebuild.
    #[serde(deserialize_with = "de_opt_u64_flexible")]
    pub after_failures: Option<u64>,
    /// Delay before the first rebuild.
    #[serde(deserialize_with = "de_opt_u64_flexible")]
    pub base_delay_ms: Option<u64>,
    /// Growth factor applied per further attempt.
    #[serde(deserialize_with = "de_opt_f64_flexible")]
    pub multiplier: Option<f64>,
    /// Ceiling on the computed delay.
    #[serde(deserialize_with = "de_opt_u64_flexible")]
    pub max_delay_ms: Option<u64>,
    /// Fraction of the delay randomised, in `0.0..=1.0`.
    #[serde(deserialize_with = "de_opt_f64_flexible")]
    pub jitter: Option<f64>,
    /// Rebuild attempts before the node gives up; `0` never gives up.
    #[serde(deserialize_with = "de_opt_u64_flexible")]
    pub max_attempts: Option<u64>,
}

impl HealConfig {
    /// Layer this block over `global` and both over `defaults`.
    pub fn resolve(&self, global: &Self, defaults: HealSettings) -> HealSettings {
        HealSettings {
            enabled: self.enabled.or(global.enabled).unwrap_or(defaults.enabled),
            after_failures: saturating_u32(
                self.after_failures.or(global.after_failures),
                defaults.after_failures,
            ),
            base_delay_ms: self
                .base_delay_ms
                .or(global.base_delay_ms)
                .unwrap_or(defaults.base_delay_ms),
            multiplier: self
                .multiplier
                .or(global.multiplier)
                .unwrap_or(defaults.multiplier),
            max_delay_ms: self
                .max_delay_ms
                .or(global.max_delay_ms)
                .unwrap_or(defaults.max_delay_ms),
            jitter: self.jitter.or(global.jitter).unwrap_or(defaults.jitter),
            max_attempts: saturating_u32(
                self.max_attempts.or(global.max_attempts),
                defaults.max_attempts,
            ),
        }
    }

    /// Reject an out-of-range key rather than clamping it, naming `what`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] for a key outside its range, or
    /// for a `max_delay_ms` below the resolved `base_delay_ms`.
    pub fn validate(&self, global: &Self, what: &str) -> Result<(), SaciError> {
        let resolved = self.resolve(global, HealSettings::default());
        if resolved.after_failures < 1 {
            return Err(SaciError::configuration(format!(
                "{what}: after_failures must be at least 1; it counts the failures \
                 that precede a rebuild"
            )));
        }
        if resolved.base_delay_ms < 1 {
            return Err(SaciError::configuration(format!(
                "{what}: base_delay_ms must be at least 1; a zero delay rebuilds in a hot loop"
            )));
        }
        if resolved.multiplier < 1.0 || resolved.multiplier.is_nan() {
            return Err(SaciError::configuration(format!(
                "{what}: multiplier must be at least 1.0, got {}",
                resolved.multiplier
            )));
        }
        if !(0.0..=1.0).contains(&resolved.jitter) {
            return Err(SaciError::configuration(format!(
                "{what}: jitter must be within 0.0..=1.0, got {}",
                resolved.jitter
            )));
        }
        if resolved.max_delay_ms < resolved.base_delay_ms {
            return Err(SaciError::configuration(format!(
                "{what}: max_delay_ms ({}) must be at least base_delay_ms ({})",
                resolved.max_delay_ms, resolved.base_delay_ms
            )));
        }
        Ok(())
    }
}

/// A declared `u64` count narrowed to the `u32` the settings carry, or the
/// default when the key was omitted. A value beyond `u32::MAX` saturates
/// rather than wrapping into a small count.
fn saturating_u32(declared: Option<u64>, default: u32) -> u32 {
    declared.map_or(default, |v| u32::try_from(v).unwrap_or(u32::MAX))
}

/// Where in a pass the runner replays a workflow's dead letters.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum DlqReplayPoint {
    /// At the head of the pass, before the first declared source is drained.
    /// A replayed letter therefore reaches its sink ahead of anything new.
    #[default]
    BeforeSources,
    /// After the last node of the pass wrote, before pacing. New arrivals go
    /// first and a replay uses what is left of the pass.
    AfterSources,
}

impl DlqReplayPoint {
    /// The KDL string this point is written as, which is also what
    /// `GET /api/dlq` reports.
    pub fn as_str(self) -> &'static str {
        match self {
            DlqReplayPoint::BeforeSources => "before_sources",
            DlqReplayPoint::AfterSources => "after_sources",
        }
    }
}

/// The store a [`DlqConfig`] names when the block names none.
fn default_dlq_store() -> String {
    "redb".to_string()
}

/// One workflow's dead letter queue: which store holds a refused batch, when
/// the runner replays what is waiting, and the config both halves of that
/// store are built from.
///
/// Every key outside `replay`, `source` and `sink` belongs to the store's
/// connector and is shared by both halves. A key only one half accepts goes
/// in that half's own block, which wins over the shared value:
///
/// ```kdl
/// workflow "orders" {
///     dlq "redb" {
///         replay "after_sources"
///         directory "/var/lib/saci/dlq"
///         source { check_integrity #true }
///     }
/// }
/// ```
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct DlqBlock {
    /// Which store backs the queue: `redb`, `kafka` or `nats`. From the
    /// block's leading argument.
    #[serde(rename = "id", default = "default_dlq_store")]
    pub store: String,
    /// Where in a pass a replay runs.
    #[serde(default)]
    pub replay: DlqReplayPoint,
    /// Keys only the store's source half takes.
    #[serde(default = "default_config")]
    pub source: ConfigValue,
    /// Keys only the store's sink half takes.
    #[serde(default = "default_config")]
    pub sink: ConfigValue,
    /// Every other key, handed to both halves.
    #[serde(flatten)]
    pub config: ConfigMap,
}

impl Default for DlqBlock {
    fn default() -> Self {
        Self {
            store: default_dlq_store(),
            replay: DlqReplayPoint::default(),
            source: default_config(),
            sink: default_config(),
            config: ConfigMap::new(),
        }
    }
}

/// A workflow's `dlq` declaration, in any of the three shapes KDL writes it.
///
/// `dlq` alone is an empty table, `dlq "redb"` a scalar and
/// `dlq "redb" { ... }` a table carrying `id`, so the scalar form is the one
/// case a derived `Deserialize` would refuse.
#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct DlqConfig(pub DlqBlock);

impl<'de> Deserialize<'de> for DlqConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Through the value tree rather than an untagged enum, so a bad key
        // inside the table form still names itself instead of collapsing
        // into "data did not match any variant".
        let value = ConfigValue::deserialize(deserializer)?;
        match value {
            ConfigValue::String(store) => Ok(DlqConfig(DlqBlock {
                store,
                ..DlqBlock::default()
            })),
            table => DlqBlock::deserialize(table)
                .map(DlqConfig)
                .map_err(serde::de::Error::custom),
        }
    }
}

/// Adaptive flow-control policy: how many rows a runner admits from a source
/// per pass, how large the Arrow chunk it hands downstream is, and how the
/// search for that size is paced.
///
/// Declared once at the top level, and overridden field by field per source:
///
/// ```kdl
/// flow_control {
///     enabled #true
///     min_rows 1024
///     max_rows 65536
///     start_rows 4096
///     max_chunk_bytes 8388608
///     target_latency_ms 250
///     adjust_interval_ms 60000
///     growth_factor 2.0
///     improve_threshold 0.05
///     min_samples_per_arm 4
///     settle_after_epochs 3
///     backoff_factor 2.0
///     backoff_cooldown 4
/// }
///
/// workflow "w" {
///     source "orders" type="nats" component="Order" {
///         flow_control { rows 4096 }
///     }
/// }
/// ```
///
/// Every key is optional and every default is a working value; the block
/// exists to pace, pin or disable adaptation, not because it has to be
/// filled in. [`resolve`](Self::resolve) layers a source's block over the
/// top-level one and both over the hard defaults.
///
/// | Key | Unit | Default | Trade-off |
/// |---|---|---|---|
/// | `enabled` | flag | `#true` | `#false` drains each source to EOF per iteration and paces unconditionally |
/// | `min_rows` | rows | 1024 | floor; raising it protects throughput on a pipeline with heavy per-pass overhead |
/// | `max_rows` | rows | 65536 | ceiling; raising it trades memory and per-pass latency for throughput |
/// | `start_rows` | rows | 4096 | where a run begins before any epoch has decided |
/// | `max_chunk_bytes` | bytes | 8 MiB | memory bound on one chunk; `0` unbounded |
/// | `target_latency_ms` | ms | 250 in stream mode, 0 in batch modes | a candidate breaching it loses; `0` pursues throughput alone |
/// | `adjust_interval_ms` | ms | 60000 | one experiment's length: longer measures more cleanly, shorter reacts sooner |
/// | `growth_factor` | ratio | 2.0 | candidate step size; must exceed 1.0 |
/// | `improve_threshold` | ratio | 0.05 | gain a candidate needs to win; lower chases noise, higher ignores real gains |
/// | `min_samples_per_arm` | passes | 4 | evidence each arm needs; higher is stricter, and an epoch with less decides nothing |
/// | `settle_after_epochs` | epochs | 3 | deciding epochs that leave the size alone before the search rests on it; `0` never rests |
/// | `backoff_factor` | ratio | 2.0 | divisor on a guard trip; must exceed 1.0 |
/// | `backoff_cooldown` | passes | 4 | passes held after a back-off; `0` resumes the search at once |
/// | `rows` | rows | none | pins the size and disables adaptation |
///
/// `rows` pins one size. Within one block it is mutually exclusive with
/// `min_rows`, `max_rows` and `start_rows`, which
/// [`validate`](Self::validate) rejects instead of resolving by precedence.
/// A top-level `rows` is inherited by every source that declares no `rows`
/// of its own, so pin globally only to pin every source.
///
/// A source on a path to a windowed node is bounded but never sliced: the
/// credit stops the runner pulling the next arrival, and the arrival in hand
/// is one whole pass, so `max_chunk_bytes` does not bound it. Bound the
/// arrival at the connector instead. See
/// [`windowing`](super::windowing) for why.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct FlowControlConfig {
    /// Whether the runner applies an admission credit at all. Default `true`;
    /// `false` drains every source to EOF per iteration and paces
    /// unconditionally.
    #[serde(deserialize_with = "de_opt_bool_flexible")]
    pub enabled: Option<bool>,
    /// Floor on the adaptive target, in rows. Default 1024.
    #[serde(deserialize_with = "de_opt_u64_flexible")]
    pub min_rows: Option<u64>,
    /// Ceiling on the adaptive target, in rows. Default 65536.
    #[serde(deserialize_with = "de_opt_u64_flexible")]
    pub max_rows: Option<u64>,
    /// Target the controller starts from, clamped into
    /// `[min_rows, max_rows]`. Default 4096.
    #[serde(deserialize_with = "de_opt_u64_flexible")]
    pub start_rows: Option<u64>,
    /// Ceiling on the projected Arrow memory of one chunk. Default 8 MiB;
    /// `0` is unbounded. Does not apply to a source on a path to a windowed
    /// node, whose arrivals are never sliced.
    #[serde(deserialize_with = "de_opt_u64_flexible")]
    pub max_chunk_bytes: Option<u64>,
    /// Latency objective for one pass. Defaults to 250 in stream mode, where
    /// per-item latency is the contract, and to 0 in `continuous`/`interval`,
    /// where throughput is the only objective. An explicit value, including
    /// `0`, wins in either mode.
    #[serde(deserialize_with = "de_opt_u64_flexible")]
    pub target_latency_ms: Option<u64>,
    /// Length of one adjustment epoch, which is both the pacing of adjustment
    /// decisions and the length of one experiment. Default 60000. Meaningless
    /// for a pinned or disabled controller, which never adjusts.
    #[serde(deserialize_with = "de_opt_u64_flexible")]
    pub adjust_interval_ms: Option<u64>,
    /// Multiplier for the candidate arm when stepping up, and its reciprocal
    /// when stepping down. Default 2.0; must be greater than 1.0.
    #[serde(deserialize_with = "de_opt_f64_flexible")]
    pub growth_factor: Option<f64>,
    /// Relative throughput gain a candidate must show to unseat the
    /// incumbent. Default 0.05; must be within `0.0..1.0`.
    #[serde(deserialize_with = "de_opt_f64_flexible")]
    pub improve_threshold: Option<f64>,
    /// Usable samples each arm needs before an epoch may decide. Default 4;
    /// must be at least 1.
    #[serde(deserialize_with = "de_opt_u64_flexible")]
    pub min_samples_per_arm: Option<u64>,
    /// Consecutive deciding epochs that leave the size alone before the
    /// controller rests on it, admitting at the incumbent only. Default 3;
    /// `0` never rests. A guard trip, an epoch that moves the size, or a size
    /// missing `target_latency_ms` starts the count again.
    #[serde(deserialize_with = "de_opt_u64_flexible")]
    pub settle_after_epochs: Option<u64>,
    /// Divisor applied to the target on a guard trip. Default 2.0; must be
    /// greater than 1.0.
    #[serde(deserialize_with = "de_opt_f64_flexible")]
    pub backoff_factor: Option<f64>,
    /// Passes held at the reduced size after a back-off before the search
    /// resumes. Default 4; `0` resumes immediately.
    #[serde(deserialize_with = "de_opt_u64_flexible")]
    pub backoff_cooldown: Option<u64>,
    /// Pin the size and disable adaptation.
    #[serde(deserialize_with = "de_opt_u64_flexible")]
    pub rows: Option<u64>,
}

/// Narrow a declared row count to `usize`, saturating rather than wrapping on
/// a 32-bit host.
fn declared_rows(declared: Option<u64>, default: usize) -> usize {
    match declared {
        Some(rows) => usize::try_from(rows).unwrap_or(usize::MAX),
        None => default,
    }
}

/// Narrow a declared count to `u32`, saturating rather than wrapping.
fn declared_count(declared: Option<u64>, default: u32) -> u32 {
    match declared {
        Some(count) => u32::try_from(count).unwrap_or(u32::MAX),
        None => default,
    }
}

impl FlowControlConfig {
    /// The concrete settings this block yields when layered over `global`,
    /// ending at `defaults`.
    ///
    /// Field by field: this block wins, then `global`, then `defaults`, which
    /// the caller picks by run mode ([`FlowSettings::default`] for the batch
    /// modes, [`FlowSettings::stream_defaults`] for stream mode).
    /// `start_rows` is clamped into the resolved `[min_rows, max_rows]` as a
    /// defensive invariant: [`validate`](Self::validate) already rejects a
    /// declared `start_rows` outside that range at load time, but a
    /// hand-built config that skipped it still gets a usable range here, and
    /// `min_rows`/`max_rows` are themselves ordered the same way.
    pub fn resolve(&self, global: &Self, defaults: FlowSettings) -> FlowSettings {
        let min_rows = declared_rows(self.min_rows.or(global.min_rows), defaults.min_rows).max(1);
        let max_rows =
            declared_rows(self.max_rows.or(global.max_rows), defaults.max_rows).max(min_rows);
        let start_rows = declared_rows(self.start_rows.or(global.start_rows), defaults.start_rows);
        FlowSettings {
            min_rows,
            max_rows,
            start_rows: start_rows.clamp(min_rows, max_rows),
            max_chunk_bytes: self
                .max_chunk_bytes
                .or(global.max_chunk_bytes)
                .unwrap_or(defaults.max_chunk_bytes),
            target_latency_ms: self
                .target_latency_ms
                .or(global.target_latency_ms)
                .unwrap_or(defaults.target_latency_ms),
            adjust_interval_ms: self
                .adjust_interval_ms
                .or(global.adjust_interval_ms)
                .unwrap_or(defaults.adjust_interval_ms),
            growth_factor: self
                .growth_factor
                .or(global.growth_factor)
                .unwrap_or(defaults.growth_factor),
            improve_threshold: self
                .improve_threshold
                .or(global.improve_threshold)
                .unwrap_or(defaults.improve_threshold),
            min_samples_per_arm: declared_count(
                self.min_samples_per_arm.or(global.min_samples_per_arm),
                defaults.min_samples_per_arm,
            ),
            settle_after_epochs: declared_count(
                self.settle_after_epochs.or(global.settle_after_epochs),
                defaults.settle_after_epochs,
            ),
            backoff_factor: self
                .backoff_factor
                .or(global.backoff_factor)
                .unwrap_or(defaults.backoff_factor),
            backoff_cooldown: declared_count(
                self.backoff_cooldown.or(global.backoff_cooldown),
                defaults.backoff_cooldown,
            ),
            fixed_rows: self
                .rows
                .or(global.rows)
                .map(|rows| declared_rows(Some(rows), 1).max(1)),
            enabled: self.enabled.or(global.enabled).unwrap_or(defaults.enabled),
        }
    }

    /// Check this block on its own and against what it inherits from
    /// `global`. The top-level block validates against
    /// `FlowControlConfig::default()`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] naming `what` and the offending key
    /// when `rows` is declared alongside `min_rows`, `max_rows` or
    /// `start_rows` in the same block, when `min_rows`, `rows`,
    /// `min_samples_per_arm` or an adaptive `adjust_interval_ms` is zero, when
    /// the effective `max_rows` is below the effective `min_rows`, when a
    /// declared `start_rows` falls outside the effective
    /// `[min_rows, max_rows]`, when `growth_factor` or `backoff_factor` is not
    /// a finite number greater than 1.0, or when `improve_threshold` is
    /// outside `0.0..1.0`.
    pub fn validate(&self, global: &Self, what: &str) -> SaciResult<()> {
        if self.rows.is_some()
            && (self.min_rows.is_some() || self.max_rows.is_some() || self.start_rows.is_some())
        {
            return Err(SaciError::configuration(format!(
                "{what}: rows pins a fixed size and cannot be combined with \
                 min_rows/max_rows/start_rows; declare one intent or the other"
            )));
        }
        let min = self.min_rows.or(global.min_rows);
        let max = self.max_rows.or(global.max_rows);
        if let Some(min) = min
            && min < 1
        {
            return Err(SaciError::configuration(format!(
                "{what}: min_rows must be at least 1"
            )));
        }
        let pinned = self.rows.or(global.rows);
        if let Some(rows) = pinned
            && rows < 1
        {
            return Err(SaciError::configuration(format!(
                "{what}: rows must be at least 1"
            )));
        }
        // Compared on the values that take effect, not only on a pair declared
        // together: `max_rows 100` alone must be rejected rather than silently
        // widened to the default floor by `resolve`.
        let defaults = FlowSettings::default();
        let effective_min = declared_rows(min, defaults.min_rows).max(1);
        let effective_max = declared_rows(max, defaults.max_rows);
        if (min.is_some() || max.is_some()) && effective_max < effective_min {
            return Err(SaciError::configuration(format!(
                "{what}: max_rows ({effective_max}) must be at least min_rows ({effective_min})"
            )));
        }
        // The range is well-ordered from here on: either both bounds were
        // declared and just passed the check above, or one or both are the
        // hard defaults, which are ordered by construction.
        let effective_max = effective_max.max(effective_min);

        // A declared `start_rows` outside the effective range is rejected
        // here, exactly like `max_rows`/`min_rows` above: an operator who
        // wrote a value the range excludes meant something this engine
        // cannot honour. `resolve`'s clamp is not this check wearing a
        // different hat. It survives only as a defensive invariant for a
        // hand-built `FlowControlConfig` that calls `resolve` directly,
        // without ever passing through `validate` at all.
        if let Some(start) = self.start_rows.or(global.start_rows) {
            let effective_start = declared_rows(Some(start), defaults.start_rows);
            if effective_start < effective_min || effective_start > effective_max {
                return Err(SaciError::configuration(format!(
                    "{what}: start_rows ({effective_start}) must be within \
                     min_rows ({effective_min})..=max_rows ({effective_max})"
                )));
            }
        }

        // The experiment's shape. An out-of-range value is rejected rather
        // than clamped: an operator who writes `growth_factor 0.5` meant
        // something this engine cannot do.
        let adaptive = pinned.is_none() && self.enabled.or(global.enabled).unwrap_or(true);
        if let Some(interval) = self.adjust_interval_ms.or(global.adjust_interval_ms)
            && interval < 1
            && adaptive
        {
            return Err(SaciError::configuration(format!(
                "{what}: adjust_interval_ms must be at least 1; it paces adjustment decisions"
            )));
        }
        for (key, factor) in [
            ("growth_factor", self.growth_factor.or(global.growth_factor)),
            (
                "backoff_factor",
                self.backoff_factor.or(global.backoff_factor),
            ),
        ] {
            if let Some(factor) = factor
                && (!factor.is_finite() || factor <= 1.0)
            {
                return Err(SaciError::configuration(format!(
                    "{what}: {key} must be a finite number greater than 1.0, got {factor}"
                )));
            }
        }
        if let Some(threshold) = self.improve_threshold.or(global.improve_threshold)
            && !(0.0..1.0).contains(&threshold)
        {
            return Err(SaciError::configuration(format!(
                "{what}: improve_threshold must be within 0.0..1.0, got {threshold}"
            )));
        }
        if let Some(samples) = self.min_samples_per_arm.or(global.min_samples_per_arm)
            && samples < 1
        {
            return Err(SaciError::configuration(format!(
                "{what}: min_samples_per_arm must be at least 1"
            )));
        }
        Ok(())
    }
}

/// Declares an IO source that feeds rows into a component column.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct SourceSpec {
    /// Mandatory id, from the node's leading argument. Unique workflow-wide.
    pub id: String,
    /// Optional display name. The dashboard shows the id when this is absent.
    #[serde(default)]
    pub name: Option<String>,
    /// Factory lookup key.
    #[serde(rename = "type")]
    pub type_name: String,
    /// Id of the `transformer` node that decodes this source's bytes. Absent
    /// for a connector that produces `RecordBatch`es directly.
    #[serde(default)]
    pub transformer: Option<String>,
    /// Name of the component this source writes into. Checked against the
    /// runtime's `declared_components()` at load time.
    pub component: String,
    /// Retry policy for every source operation. Omitted block uses the
    /// defaults (4 attempts, exponential backoff).
    #[serde(default)]
    pub retry: RetryConfig,
    /// Self-healing override for this source, layered field by field over
    /// the top-level `heal` block. Absent means the top-level policy.
    ///
    /// Declaring the block on a connector that cannot be rebuilt is a
    /// load-time error rather than a block that quietly does nothing.
    #[serde(default)]
    pub heal: Option<HealConfig>,
    /// Flow-control override for this source, layered field by field over the
    /// top-level `flow_control` block. Absent means the top-level policy.
    #[serde(default)]
    pub flow_control: Option<FlowControlConfig>,
    /// Opaque per-source configuration.
    #[serde(default = "default_config")]
    pub config: ConfigValue,
}

/// Declares an IO sink that drains rows from a component column.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct SinkSpec {
    /// Mandatory id, from the node's leading argument. Unique workflow-wide.
    pub id: String,
    /// Optional display name. The dashboard shows the id when this is absent.
    #[serde(default)]
    pub name: Option<String>,
    /// Factory lookup key.
    #[serde(rename = "type")]
    pub type_name: String,
    /// Id of the `transformer` node that encodes this sink's bytes. Absent
    /// for a connector that consumes `RecordBatch`es directly.
    #[serde(default)]
    pub transformer: Option<String>,
    /// Name of the component this sink reads from. Checked against the
    /// runtime's `declared_components()` at load time.
    pub component: String,
    /// Retry policy for every sink operation. Omitted block uses the
    /// defaults (4 attempts, exponential backoff).
    #[serde(default)]
    pub retry: RetryConfig,
    /// Self-healing override for this sink, layered field by field over the
    /// top-level `heal` block. Absent means the top-level policy.
    ///
    /// Declaring the block on a connector that cannot be rebuilt is a
    /// load-time error rather than a block that quietly does nothing.
    #[serde(default)]
    pub heal: Option<HealConfig>,
    /// Opaque per-sink configuration.
    #[serde(default = "default_config")]
    pub config: ConfigValue,
}

/// An explicit directed edge between two declared node ids.
///
/// `from` and `to` name a `source`, `wasm`, `plugin` or `sink` id, never a
/// `transformer` id, which is not a graph node.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct LinkSpec {
    /// The upstream node id.
    pub from: String,
    /// The downstream node id.
    pub to: String,
    /// Branch name this link carries. A processor's `run-result.routes`
    /// selects the links its output is delivered to by this name. Absent =
    /// unlabelled: never selected by a routing decision, delivered only under
    /// legacy multicast.
    #[serde(default)]
    pub branch: Option<String>,
}

/// Which of the three graph roles a declared node plays.
///
/// Distinct from a node's KDL kind (`source`/`wasm`/`plugin`/`sink`): both
/// `wasm` and `plugin` nodes are [`NodeKind::Processor`], since a link treats
/// them identically.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeKind {
    Source,
    Processor,
    Sink,
}

/// One `workflow`: the whole DAG of sources, processors, sinks and
/// transformers, wired together by explicit [`LinkSpec`] declarations.
///
/// Every declared transformer, source, `wasm`, `plugin` and sink id shares one
/// namespace and must be unique; see `WorkflowSpec::validate` for the full
/// set of load-time graph rules.
///
/// The `wasm` and `plugin` fields are declared unconditionally, like the
/// `mode "cluster"` half of [`ServiceMode`]: a build without those hosts
/// still parses a file that declares one, so
/// [`validate_build_capabilities`](super::validation::validate_build_capabilities)
/// can name the `--features` flag that would run it instead of serde
/// reporting `wasm` as an unknown key.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct WorkflowSpec {
    /// Mandatory id, from the node's leading argument.
    pub id: String,
    /// Optional display name. The dashboard shows the id when this is absent.
    #[serde(default)]
    pub name: Option<String>,
    /// Declared byte formats, one `transformer` node each.
    #[serde(rename = "transformer", default, deserialize_with = "one_or_many")]
    pub transformers: Vec<TransformerSpec>,
    /// Data sources feeding the workflow. One `source` node each.
    #[serde(rename = "source", default, deserialize_with = "one_or_many")]
    pub sources: Vec<SourceSpec>,
    /// WASM processor nodes, one `wasm` node each. Needs `--features wasm`
    /// to build; see the type docs for why it parses without it.
    #[serde(default, deserialize_with = "one_or_many")]
    pub wasm: Vec<WasmSpec>,
    /// Native plugin processor nodes, one `plugin` node each. Needs
    /// `--features plugin` to build.
    #[serde(default, deserialize_with = "one_or_many")]
    pub plugin: Vec<PluginSpec>,
    /// Data sinks draining the workflow. One `sink` node each.
    #[serde(rename = "sink", default, deserialize_with = "one_or_many")]
    pub sinks: Vec<SinkSpec>,
    /// Explicit edges between declared nodes. One `link` node each.
    #[serde(rename = "link", default, deserialize_with = "one_or_many")]
    pub links: Vec<LinkSpec>,
    /// This workflow's dead letter queue. Absent means a batch a sink
    /// refuses is logged and dropped, which is the behaviour with no `dlq`
    /// block at all.
    #[serde(default)]
    pub dlq: Option<DlqConfig>,
}

/// `^[A-Za-z0-9][A-Za-z0-9_-]*$`, at most 64 bytes.
///
/// Ids are used verbatim as OpenTelemetry attribute values and as topology
/// node ids, so the charset is closed rather than escaped.
fn is_valid_id(id: &str) -> bool {
    if id.is_empty() || id.len() > 64 {
        return false;
    }
    let mut chars = id.chars();
    let first_is_alphanumeric = chars.next().is_some_and(|c| c.is_ascii_alphanumeric());
    first_is_alphanumeric
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

impl WorkflowSpec {
    /// Every declared node's id and kind, in declaration order: sources, then
    /// wasm processors, then plugin processors, then sinks.
    pub(crate) fn nodes(&self) -> Vec<(&str, NodeKind)> {
        let mut out = Vec::new();
        for s in &self.sources {
            out.push((s.id.as_str(), NodeKind::Source));
        }
        for w in &self.wasm {
            out.push((w.id.as_str(), NodeKind::Processor));
        }
        for p in &self.plugin {
            out.push((p.id.as_str(), NodeKind::Processor));
        }
        for s in &self.sinks {
            out.push((s.id.as_str(), NodeKind::Sink));
        }
        out
    }

    /// Every declared id outside `nodes()` too (transformers), each paired
    /// with the KDL node kind that declared it, for id-validation messages.
    fn declared_ids(&self) -> Vec<(&str, &'static str)> {
        let mut out = Vec::new();
        for t in &self.transformers {
            out.push((t.id.as_str(), "transformer"));
        }
        for (id, kind) in self.nodes() {
            let label = match kind {
                NodeKind::Source => "source",
                NodeKind::Processor => "processor",
                NodeKind::Sink => "sink",
            };
            out.push((id, label));
        }
        out
    }

    /// Node indices in topological order, so a node always follows every node
    /// that links into it.
    ///
    /// # Errors
    ///
    /// `SaciError::Configuration` when the links contain a cycle.
    pub(crate) fn topological_order(&self) -> SaciResult<Vec<usize>> {
        let nodes = self.nodes();
        let n = nodes.len();
        let id_to_idx: HashMap<&str, usize> = nodes
            .iter()
            .enumerate()
            .map(|(i, (id, _))| (*id, i))
            .collect();

        // adjacency[i] = list of node indices that depend on i (i.e. i → dependent).
        let mut adjacency: Vec<Vec<usize>> = vec![Vec::new(); n];
        let mut in_degree: Vec<usize> = vec![0; n];
        for link in &self.links {
            let (Some(&from), Some(&to)) = (
                id_to_idx.get(link.from.as_str()),
                id_to_idx.get(link.to.as_str()),
            ) else {
                // Unknown ids are rejected by `validate`'s rule 5; this
                // function is also called from within `validate` itself
                // (rule 8), before rule 5 in a hand-built (non-KDL) spec, so
                // skip rather than panic.
                continue;
            };
            adjacency[from].push(to);
            in_degree[to] += 1;
        }

        // Kahn's algorithm, draining the whole frontier each round so the
        // result is flattened frontier-by-frontier, keeping declaration order
        // inside each depth.
        let mut queue: VecDeque<usize> = (0..n).filter(|&i| in_degree[i] == 0).collect();
        let mut order: Vec<usize> = Vec::with_capacity(n);
        let mut visited = 0usize;

        while !queue.is_empty() {
            let frontier: Vec<usize> = queue.drain(..).collect();
            visited += frontier.len();

            let mut next_queue: VecDeque<usize> = VecDeque::new();
            for &node in &frontier {
                order.push(node);
                for &dep in &adjacency[node] {
                    in_degree[dep] -= 1;
                    if in_degree[dep] == 0 {
                        next_queue.push_back(dep);
                    }
                }
            }
            queue = next_queue;
        }

        if visited != n {
            return Err(SaciError::configuration(format!(
                "workflow '{}': links contain a cycle",
                self.id
            )));
        }

        Ok(order)
    }

    /// Validate every load-time graph rule.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] describing the first violation
    /// found, in the order documented on the type.
    pub(crate) fn validate(&self, mode: &ServiceMode) -> SaciResult<()> {
        let wf = &self.id;

        // 1. The workflow declares at least one node.
        let nodes = self.nodes();
        if nodes.is_empty() {
            return Err(SaciError::configuration(format!(
                "workflow '{wf}': declares no source, processor or sink node"
            )));
        }

        // 2. Every id matches the closed charset and length bound.
        if !is_valid_id(wf) {
            return Err(SaciError::configuration(format!(
                "workflow: id '{wf}' is invalid; ids must match \
                 ^[A-Za-z0-9][A-Za-z0-9_-]*$ and be at most 64 bytes"
            )));
        }
        let declared = self.declared_ids();
        for (id, kind) in &declared {
            if !is_valid_id(id) {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': {kind} id '{id}' is invalid; ids must match \
                     ^[A-Za-z0-9][A-Za-z0-9_-]*$ and be at most 64 bytes"
                )));
            }
        }

        // 3. No id is declared twice anywhere in the workflow.
        let mut seen: HashMap<&str, &str> = HashMap::new();
        for (id, kind) in &declared {
            if let Some(&prev_kind) = seen.get(id) {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': id '{id}' is declared twice, as {prev_kind} and as {kind}"
                )));
            }
            seen.insert(id, kind);
        }

        // 4. Every source.transformer / sink.transformer names a declared
        //    transformer id.
        let transformer_ids: HashSet<&str> =
            self.transformers.iter().map(|t| t.id.as_str()).collect();
        for s in &self.sources {
            if let Some(t) = &s.transformer
                && !transformer_ids.contains(t.as_str())
            {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': source '{}' names transformer '{t}', \
                     which is not declared",
                    s.id
                )));
            }
        }
        for s in &self.sinks {
            if let Some(t) = &s.transformer
                && !transformer_ids.contains(t.as_str())
            {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': sink '{}' names transformer '{t}', which is not declared",
                    s.id
                )));
            }
        }

        // 5. Every link.from / link.to names a declared source, wasm, plugin
        //    or sink id, and from != to. A transformer id is not a node, so
        //    it is rejected here too.
        let node_kind: HashMap<&str, NodeKind> =
            nodes.iter().map(|&(id, kind)| (id, kind)).collect();
        for link in &self.links {
            if link.from == link.to {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': link '{}' -> '{}' links a node to itself",
                    link.from, link.to
                )));
            }
            if !node_kind.contains_key(link.from.as_str()) {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': link names undeclared node '{}'",
                    link.from
                )));
            }
            if !node_kind.contains_key(link.to.as_str()) {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': link names undeclared node '{}'",
                    link.to
                )));
            }
        }

        // 6. No (from, to) pair is declared twice.
        let mut seen_edges: HashSet<(&str, &str)> = HashSet::new();
        for link in &self.links {
            if !seen_edges.insert((link.from.as_str(), link.to.as_str())) {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': link '{}' -> '{}' is declared twice",
                    link.from, link.to
                )));
            }
        }

        // 7. The edge-kind matrix: a source has no input, a sink has no
        //    output.
        for link in &self.links {
            if node_kind[link.to.as_str()] == NodeKind::Source {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': link '{}' -> '{}' targets source '{}'; \
                     a source has no input",
                    link.from, link.to, link.to
                )));
            }
            if node_kind[link.from.as_str()] == NodeKind::Sink {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': link '{}' -> '{}' starts at sink '{}'; \
                     a sink has no output",
                    link.from, link.to, link.from
                )));
            }
        }

        // 8. The graph is acyclic.
        self.topological_order()?;

        // 9. Every source has at least one outbound link, and every sink at
        //    least one inbound link. A processor needs neither.
        for s in &self.sources {
            if !self.links.iter().any(|l| l.from == s.id) {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': source '{}' has no outbound link",
                    s.id
                )));
            }
        }
        for s in &self.sinks {
            if !self.links.iter().any(|l| l.to == s.id) {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': sink '{}' has no inbound link",
                    s.id
                )));
            }
        }

        // 10. Cluster mode: exactly one processor node, zero sources, zero
        //     sinks and zero links.
        if matches!(mode, ServiceMode::Cluster { .. }) {
            let processor_count = nodes
                .iter()
                .filter(|&&(_, kind)| kind == NodeKind::Processor)
                .count();
            if processor_count != 1
                || !self.sources.is_empty()
                || !self.sinks.is_empty()
                || !self.links.is_empty()
            {
                return Err(SaciError::configuration(format!(
                    "cluster mode runs exactly one 'wasm' or 'plugin' node with no source, \
                     sink or link ({} node(s), {} link(s) declared)",
                    nodes.len(),
                    self.links.len()
                )));
            }
            // The service-level window block tracks a watermark from the
            // node's inbound links, and cluster mode has none: the distributed
            // runner drives the one processor straight from partition claims.
            // Reject the block rather than silently not tracking anything.
            if let Some(w) = self.wasm.iter().find(|w| w.window.is_some()) {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': cluster mode cannot honour wasm node '{}' window \
                     block; cluster-mode windowing lives in the processor's own pipeline \
                     (WindowedSystem + WindowAccumulator)",
                    w.id
                )));
            }
            if let Some(p) = self.plugin.iter().find(|p| p.window.is_some()) {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': cluster mode cannot honour plugin node '{}' window \
                     block; cluster-mode windowing lives in the processor's own pipeline \
                     (WindowedSystem + WindowAccumulator)",
                    p.id
                )));
            }
        }

        // 11. `run_mode kind="stream"`: at least one source in the whole
        //     workflow.
        let stream_mode = matches!(
            mode,
            ServiceMode::Standalone { config } if config.run_mode == RunMode::Stream
        );
        if stream_mode && self.sources.is_empty() {
            return Err(SaciError::configuration(format!(
                "stream run mode requires at least one 'source' node ({}) declared",
                self.sources.len()
            )));
        }

        // 13. Outside stream mode, no source is live.
        if !stream_mode && let Some(live) = self.sources.iter().find(|s| is_live_source(s)) {
            return Err(SaciError::configuration(format!(
                "source type '{}' never reaches EOF; it requires standalone mode \
                 with run_mode kind=\"stream\"",
                live.type_name
            )));
        }

        // Every declared source and sink carries a valid retry block.
        for spec in &self.sources {
            spec.retry
                .validate(&format!("workflow '{wf}' source '{}'", spec.id))?;
        }
        for spec in &self.sinks {
            spec.retry
                .validate(&format!("workflow '{wf}' sink '{}'", spec.id))?;
        }

        // 14. Every present link branch is a valid id.
        for link in &self.links {
            if let Some(branch) = &link.branch
                && !is_valid_id(branch)
            {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': link '{}' -> '{}' branch '{branch}' is invalid; \
                     branches must match ^[A-Za-z0-9][A-Za-z0-9_-]*$ and be at most 64 bytes",
                    link.from, link.to
                )));
            }
        }

        // 15. A labelled link must start at a processor.
        for link in &self.links {
            if let Some(branch) = &link.branch
                && node_kind[link.from.as_str()] != NodeKind::Processor
            {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': link '{}' -> '{}' carries branch '{branch}' but '{}' is \
                     a source; only a processor can route",
                    link.from, link.to, link.from
                )));
            }
        }

        // 16. Per node, either every outbound link carries a branch or none
        //     do.
        for &(id, _) in &nodes {
            let mut labelled = false;
            let mut unlabelled = false;
            for link in &self.links {
                if link.from != id {
                    continue;
                }
                if link.branch.is_some() {
                    labelled = true;
                } else {
                    unlabelled = true;
                }
            }
            if labelled && unlabelled {
                return Err(SaciError::configuration(format!(
                    "workflow '{wf}': node '{id}' mixes labelled and unlabelled outbound \
                     links; label every link or none"
                )));
            }
        }

        // 17. A declared `window` block must be geometrically sane. Checked
        //     whether or not this build carries the node's host or the
        //     windowing engine: a bad geometry is a defect in the file either
        //     way, and the capability refusal follows in
        //     `validate_build_capabilities`.
        for w in &self.wasm {
            validate_window_block(wf, "wasm", &w.id, w.window.as_ref())?;
        }
        for p in &self.plugin {
            validate_window_block(wf, "plugin", &p.id, p.window.as_ref())?;
        }

        // 18. A declared `dlq` block must name a store this crate knows. The
        //     store's own keys are the connector's to reject, by name, when
        //     the workflow is built.
        if let Some(dlq) = &self.dlq
            && super::dlq::store_kind(&dlq.0.store).is_none()
        {
            return Err(SaciError::configuration(format!(
                "workflow '{wf}': dlq store '{}' is not one of {}",
                dlq.0.store,
                super::dlq::store_names()
            )));
        }

        Ok(())
    }
}

/// Run a declared [`WindowConfig`]'s sanity checks, naming the workflow, node
/// kind and id in the error.
///
/// Runs in every build. The geometry is config shape, which no feature
/// changes, so `size_ms=0` is refused by name wherever it is read; whether
/// this binary can *serve* the block is
/// [`validate_build_capabilities`](super::validation::validate_build_capabilities)'s
/// question.
fn validate_window_block(
    workflow_id: &str,
    kind: &str,
    id: &str,
    window: Option<&WindowConfig>,
) -> SaciResult<()> {
    let Some(window) = window else {
        return Ok(());
    };
    window.validate().map_err(|msg| {
        SaciError::configuration(format!(
            "workflow '{workflow_id}': {kind} node '{id}' window is invalid: {msg}"
        ))
    })
}

fn default_http_bind() -> String {
    "0.0.0.0:8080".to_string()
}

fn default_http_control() -> bool {
    true
}

/// HTTP control-plane configuration.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HttpConfig {
    /// Socket address to bind the HTTP server on.
    #[serde(default = "default_http_bind")]
    pub bind: String,
    /// Disable the HTTP control plane entirely.
    #[serde(default)]
    pub disabled: bool,
    /// Mount the workflow lifecycle endpoints (`/api/workflows*`).
    ///
    /// On by default, for the same reason `observability.inspector.enabled`
    /// and `.ui` are: this port already serves the topology, the connector
    /// option allowlist and the whole log tail unauthenticated, so the
    /// exposure posture is unchanged. `control #false` is the documented
    /// opt-out and drops the routes from the router entirely. Standalone mode
    /// only: a cluster node declares one workflow and stopping it is stopping
    /// the node, so the routes are never mounted there.
    #[serde(default = "default_http_control")]
    pub control: bool,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            bind: default_http_bind(),
            disabled: false,
            control: default_http_control(),
        }
    }
}

/// Log output format.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    /// Human-readable output for TTY / development.
    #[default]
    Pretty,
    /// Structured JSON for production log aggregators.
    Json,
}

fn default_log_level() -> String {
    "error".to_string()
}

fn default_sample_ratio() -> f64 {
    1.0
}

/// Observability (logging) configuration.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ObservabilityConfig {
    /// Log output format.
    #[serde(default)]
    pub log_format: LogFormat,
    /// Tracing level filter string (`"info"`, `"debug"`, etc.).
    #[serde(default = "default_log_level")]
    pub log_level: String,
    /// OTLP/HTTP collector base URL, for example
    /// `http://127.0.0.1:4318`. `None` disables span export.
    #[serde(default)]
    pub otlp_endpoint: Option<String>,
    /// Fraction of spans and events below ERROR that `log_level` admits and
    /// the subscriber keeps, 0.0 to 1.0. Decided once per root span or root
    /// event; children follow their root.
    #[serde(default = "default_sample_ratio")]
    pub sample_ratio: f64,
    /// Fraction of ERROR-level spans and events kept, 0.0 to 1.0. Independent
    /// of `sample_ratio`: an error inside a trace `sample_ratio` dropped is
    /// still rolled against this ratio.
    #[serde(default = "default_sample_ratio")]
    pub error_sample_ratio: f64,
    /// In-process telemetry capture, its JSON API and the `/ui` dashboard.
    #[serde(default)]
    pub inspector: crate::inspector::InspectorConfig,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            log_format: LogFormat::Pretty,
            log_level: default_log_level(),
            otlp_endpoint: None,
            sample_ratio: default_sample_ratio(),
            error_sample_ratio: default_sample_ratio(),
            inspector: crate::inspector::InspectorConfig::default(),
        }
    }
}

impl ObservabilityConfig {
    /// Validate the two sampling ratios.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] naming the first key whose value
    /// falls outside `0.0..=1.0`. NaN fails the range check too, so it is
    /// rejected rather than silently treated as "never".
    pub fn validate(&self) -> SaciResult<()> {
        for (key, value) in [
            ("sample_ratio", self.sample_ratio),
            ("error_sample_ratio", self.error_sample_ratio),
        ] {
            if !(0.0..=1.0).contains(&value) {
                return Err(SaciError::configuration(format!(
                    "observability.{key} must be between 0.0 and 1.0"
                )));
            }
        }
        Ok(())
    }
}

/// Top-level service configuration.
///
/// Load from a KDL file with [`ServiceConfig::load`]:
///
/// ```no_run
/// # #[cfg(feature = "service")]
/// # {
/// use saci_service::service::config::ServiceConfig;
/// let cfg = ServiceConfig::load("saci.kdl").unwrap();
/// # }
/// ```
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ServiceConfig {
    /// Node identity and storage.
    pub node: NodeConfig,
    /// Runtime mode (standalone or cluster), flattened into the document.
    #[serde(flatten)]
    pub mode: ServiceMode,
    /// Persistent store declaration; `None` keeps the in-process/redb
    /// backends as today.
    #[serde(default)]
    pub store: Option<StoreConfig>,
    /// The workflows this process runs. One or more `workflow` blocks; a
    /// single block deserializes as a one-element list.
    #[serde(rename = "workflow", deserialize_with = "one_or_many")]
    pub workflows: Vec<WorkflowSpec>,
    /// HTTP control-plane options.
    #[serde(default)]
    pub http: HttpConfig,
    /// Logging / observability options.
    #[serde(default)]
    pub observability: ObservabilityConfig,
    /// Adaptive flow control for every source. An omitted block is the
    /// enabled defaults.
    #[serde(default)]
    pub flow_control: FlowControlConfig,
    /// Connector self-healing for every source and sink. An omitted block is
    /// the enabled defaults.
    #[serde(default)]
    pub heal: HealConfig,
    /// Names usable as `${name}` placeholders anywhere in this file. A
    /// declared name wins over a same-named process env var, so the file is
    /// self-contained.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub variables: HashMap<String, String>,
}

/// Pull the `variables` node out of an unsubstituted parse. Returns an empty
/// map when the file declares none. Names are restricted to `[A-Za-z0-9_]`
/// so a typo in a `${...}` reference cannot alias a name the scanner would
/// not recognise.
fn extract_declared_variables(value: &ConfigValue) -> SaciResult<HashMap<String, String>> {
    let Some(vars) = value.get("variables") else {
        return Ok(HashMap::new());
    };
    let declared =
        serde_json::from_value::<HashMap<String, String>>(vars.clone()).map_err(|e| {
            SaciError::configuration(format!("variables block must be name/string pairs: {e}"))
        })?;
    for name in declared.keys() {
        if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(SaciError::configuration(format!(
                "invalid variable name {name:?}: use [A-Za-z0-9_]"
            )));
        }
    }
    Ok(declared)
}

impl ServiceConfig {
    /// Load a [`ServiceConfig`] from a KDL file at `path`.
    ///
    /// The file is read, its declared `variables` and env-var placeholders
    /// are substituted (declared names win over same-named env vars), the
    /// KDL is parsed, and semantic validation is applied before returning.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] for IO failures, parse errors, or
    /// any validation constraint violation.
    pub fn load(path: impl AsRef<std::path::Path>) -> SaciResult<Self> {
        let raw = std::fs::read_to_string(path.as_ref()).map_err(|e| {
            SaciError::configuration(format!(
                "reading config file {}: {e}",
                path.as_ref().display()
            ))
        })?;
        let value_pre = saci_config::from_kdl_str(&raw)?;
        let declared = extract_declared_variables(&value_pre)?;
        let value = saci_config::from_kdl_str_with_vars(&raw, &declared)?;
        let config = ServiceConfig::deserialize(value)
            .map_err(|e| SaciError::configuration(format!("parsing config: {e}")))?;
        config.validate()?;
        Ok(config)
    }

    /// Validate semantic constraints that serde cannot enforce.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] describing the first constraint
    /// violation found.
    pub fn validate(&self) -> SaciResult<()> {
        if self.node.data_dir.as_os_str().is_empty() {
            return Err(SaciError::configuration("node.data_dir must not be empty"));
        }

        self.observability.validate()?;

        if let ServiceMode::Cluster { config } = &self.mode {
            if config.peers.is_empty() {
                return Err(SaciError::configuration(
                    "cluster mode requires at least one peer",
                ));
            }

            let mut seen_ids: HashSet<u64> = HashSet::new();
            for peer in &config.peers {
                if !seen_ids.insert(peer.id) {
                    return Err(SaciError::configuration(format!(
                        "cluster peers contain duplicate id: {}",
                        peer.id
                    )));
                }
            }

            let node_id = self.node.id;
            if !config.peers.iter().any(|p| p.id == node_id) {
                return Err(SaciError::configuration(format!(
                    "node id {node_id} is not listed in cluster.peers"
                )));
            }

            let min_lease = config.election_timeout_ms.saturating_mul(3);
            if config.lease_ttl_ms < min_lease {
                return Err(SaciError::configuration(format!(
                    "lease_ttl_ms ({}) must be >= 3 × election_timeout_ms ({}) = {}",
                    config.lease_ttl_ms, config.election_timeout_ms, min_lease,
                )));
            }

            if config.snapshot_log_interval == 0 {
                return Err(SaciError::configuration(
                    "snapshot_log_interval must be at least 1; a zero interval snapshots \
                     on every committed entry",
                ));
            }
        }

        for wf in &self.workflows {
            wf.validate(&self.mode)?;
        }

        // Cross-workflow constraints, enforced after every workflow passes
        // its own graph rules.
        //
        // Cluster mode runs one distributed group per process: exactly one
        // workflow may be declared.
        if matches!(self.mode, ServiceMode::Cluster { .. }) && self.workflows.len() != 1 {
            return Err(SaciError::configuration(format!(
                "cluster mode requires exactly one workflow; found {}",
                self.workflows.len()
            )));
        }

        // Workflow ids are process-unique so topology and logs can name a
        // workflow without ambiguity.
        let mut seen_workflows: HashSet<&str> = HashSet::new();
        for wf in &self.workflows {
            if !seen_workflows.insert(wf.id.as_str()) {
                return Err(SaciError::configuration(format!(
                    "workflow id '{}' is declared twice",
                    wf.id
                )));
            }
        }

        // Node ids are process-unique across workflows: the OTel attribution
        // keys (`source=`/`processor=`/`sink=`) are bare node ids, so an
        // overlap would double-count metrics.
        let mut node_owners: HashMap<&str, (&str, &str)> = HashMap::new();
        for wf in &self.workflows {
            for (id, kind) in wf.declared_ids() {
                if let Some(&(prev_kind, prev_wf)) = node_owners.get(id) {
                    return Err(SaciError::configuration(format!(
                        "node id '{id}' is declared in workflow '{prev_wf}' as {prev_kind} \
                         and in workflow '{}' as {kind}; node ids must be unique across \
                         all workflows",
                        wf.id
                    )));
                }
                node_owners.insert(id, (kind, wf.id.as_str()));
            }
        }

        // Channel names pair exactly one ChannelSink with one ChannelSource.
        // A dangling half hangs: a source whose registry-held sender never
        // drops, or a sink whose registry-held receiver is never drained.
        let mut channels: HashMap<&str, (Option<&str>, Option<&str>)> = HashMap::new();
        for wf in &self.workflows {
            for s in &wf.sources {
                if s.type_name == "ChannelSource"
                    && let Some(name) = s.config.get("name").and_then(ConfigValue::as_str)
                {
                    let entry = channels.entry(name).or_default();
                    if entry.0.is_some() {
                        return Err(SaciError::configuration(format!(
                            "channel '{name}': more than one ChannelSource declared"
                        )));
                    }
                    entry.0 = Some(s.id.as_str());
                }
            }
            for s in &wf.sinks {
                if s.type_name == "ChannelSink"
                    && let Some(name) = s.config.get("name").and_then(ConfigValue::as_str)
                {
                    let entry = channels.entry(name).or_default();
                    if entry.1.is_some() {
                        return Err(SaciError::configuration(format!(
                            "channel '{name}': more than one ChannelSink declared"
                        )));
                    }
                    entry.1 = Some(s.id.as_str());
                }
            }
        }
        for (name, (source, sink)) in &channels {
            match (source, sink) {
                (Some(_), None) => {
                    return Err(SaciError::configuration(format!(
                        "channel '{name}': declares a ChannelSource but no ChannelSink"
                    )));
                }
                (None, Some(_)) => {
                    return Err(SaciError::configuration(format!(
                        "channel '{name}': declares a ChannelSink but no ChannelSource"
                    )));
                }
                (Some(_), Some(_)) => {}
                (None, None) => unreachable!("an entry exists only when a half was seen"),
            }
        }

        // The HTTP bind address is validated before the store, so a config
        // with both problems reports the listener first.
        if !self.http.disabled {
            SocketAddr::from_str(&self.http.bind).map_err(|e| {
                SaciError::configuration(format!(
                    "http.bind '{}' is not a valid socket address: {e}",
                    self.http.bind
                ))
            })?;
        }

        // Cluster state lives in the raft-replicated `cluster-app.redb` under
        // `node.data_dir`, so a `store` block there would name a second,
        // unreplicated home for the same data. Checked after the workflow
        // rules so a config with both problems reports the shape error first.
        if matches!(self.mode, ServiceMode::Cluster { .. }) && self.store.is_some() {
            return Err(SaciError::configuration(
                "mode \"cluster\" does not take a `store` block: cluster state lives in \
                 node.data_dir",
            ));
        }

        // A cluster workflow declares no `source` node: it ingests through
        // `PartitionSource`'s claim-and-lease mechanism, so there is no
        // admission for a controller to govern and the cluster runner builds
        // no `FlowPlan` at all. Refused rather than ignored, so a block that
        // would do nothing is never mistaken for one that works.
        if matches!(self.mode, ServiceMode::Cluster { .. })
            && self.flow_control != FlowControlConfig::default()
        {
            return Err(SaciError::configuration(
                "mode \"cluster\" does not take a `flow_control` block: a cluster workflow \
                 declares no source node, so there is no admission to govern",
            ));
        }

        // The block's own values, after the cluster refusal above: a cluster
        // config that also mis-sets a key must hear that the block does not
        // belong there, not that one of its numbers is out of range.
        self.flow_control
            .validate(&FlowControlConfig::default(), "flow_control")?;
        for wf in &self.workflows {
            for source in &wf.sources {
                if let Some(local) = &source.flow_control {
                    local.validate(
                        &self.flow_control,
                        &format!("workflow '{}' source '{}': flow_control", wf.id, source.id),
                    )?;
                }
            }
        }

        // A cluster workflow declares no source or sink node either, so
        // there is no connector for a heal policy to replace. Refused rather
        // than ignored, for the same reason as `flow_control`.
        if matches!(self.mode, ServiceMode::Cluster { .. }) && self.heal != HealConfig::default() {
            return Err(SaciError::configuration(
                "mode \"cluster\" does not take a `heal` block: a cluster workflow \
                 declares no source or sink node, so there is no connector to rebuild",
            ));
        }
        self.heal.validate(&HealConfig::default(), "heal")?;
        for wf in &self.workflows {
            for source in &wf.sources {
                if let Some(local) = &source.heal {
                    local.validate(
                        &self.heal,
                        &format!("workflow '{}' source '{}': heal", wf.id, source.id),
                    )?;
                }
            }
            for sink in &wf.sinks {
                if let Some(local) = &sink.heal {
                    local.validate(
                        &self.heal,
                        &format!("workflow '{}' sink '{}': heal", wf.id, sink.id),
                    )?;
                }
            }
        }

        // Same reason once more: a dead letter is a batch a sink refused, and
        // a cluster workflow declares no sink node.
        if matches!(self.mode, ServiceMode::Cluster { .. })
            && let Some(wf) = self.workflows.iter().find(|wf| wf.dlq.is_some())
        {
            return Err(SaciError::configuration(format!(
                "mode \"cluster\" does not take a `dlq` block (workflow '{}'): a cluster \
                 workflow declares no sink node, so there is nothing to dead-letter",
                wf.id
            )));
        }

        if let Some(StoreConfig::Redb { path, .. }) = &self.store
            && path.as_os_str().is_empty()
        {
            return Err(SaciError::configuration(
                "store redb: path must not be empty",
            ));
        }
        Ok(())
    }
}

/// A source that never reports EOF, so only the stream runner may drive it.
///
/// A compacted `KafkaSource` is one-shot regardless of `stop_at_end`: it
/// always reaches EOF once its snapshot is read.
fn is_live_source(spec: &SourceSpec) -> bool {
    let config_flag = |key: &str| {
        spec.config
            .get(key)
            .and_then(ConfigValue::as_bool)
            .unwrap_or(false)
    };
    match spec.type_name.as_str() {
        "tcp" | "saci" => true,
        "KafkaSource" => !(config_flag("stop_at_end") || config_flag("compacted")),
        "NatsSource" => !config_flag("stop_at_end"),
        _ => false,
    }
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// Parse a fixture the way [`ServiceConfig::load`] does, minus the file
    /// read. The error is flattened to a `String` so a test can assert on the
    /// text without naming the value tree's error type.
    fn parse_as<T: serde::de::DeserializeOwned>(raw: &str) -> Result<T, String> {
        let value = saci_config::from_kdl_str(raw).map_err(|e| e.to_string())?;
        T::deserialize(value).map_err(|e| e.to_string())
    }

    fn parse(raw: &str) -> Result<ServiceConfig, String> {
        parse_as(raw)
    }

    /// A trivial, feature-independent one-link workflow: a source straight to
    /// a sink. Valid under every WorkflowSpec::validate rule regardless of
    /// which of `wasm`/`plugin` are compiled in.
    const TRIVIAL_WORKFLOW: &str = r#"
workflow "w" {
    source "in" type="NoopSource" component="X"
    sink "out" type="NoopSink" component="X"
    link from="in" to="out"
}
"#;

    fn minimal_standalone_kdl() -> String {
        format!(
            r#"
mode "standalone"

node id=1 data_dir="/tmp/saci-test"
{TRIVIAL_WORKFLOW}
"#
        )
    }

    fn minimal_cluster_kdl() -> String {
        format!(
            r#"
mode "cluster"
bootstrap #true

node id=1 data_dir="/tmp/saci-cluster"

peer id=1 addr="127.0.0.1:9000"
peer id=2 addr="127.0.0.2:9000"
{TRIVIAL_WORKFLOW}
"#
        )
    }

    fn full_config() -> ServiceConfig {
        ServiceConfig {
            heal: Default::default(),
            flow_control: Default::default(),
            node: NodeConfig {
                id: 1,
                name: Some("node-1".to_string()),
                data_dir: PathBuf::from("/tmp/saci"),
            },
            mode: ServiceMode::Standalone {
                config: StandaloneConfig {
                    run_mode: RunMode::Interval { interval_ms: 5_000 },
                },
            },
            workflows: vec![WorkflowSpec {
                id: "payments".to_string(),
                name: None,
                transformers: Vec::new(),
                sources: vec![SourceSpec {
                    heal: None,
                    flow_control: None,
                    id: "kafka_in".to_string(),
                    name: None,
                    type_name: "MongoSource".to_string(),
                    transformer: None,
                    component: "orders".to_string(),
                    retry: RetryConfig::default(),
                    config: default_config(),
                }],
                wasm: Vec::new(),
                plugin: Vec::new(),
                sinks: vec![SinkSpec {
                    heal: None,
                    id: "pg_out".to_string(),
                    name: None,
                    type_name: "PostgresSink".to_string(),
                    transformer: None,
                    component: "orders".to_string(),
                    retry: RetryConfig::default(),
                    config: default_config(),
                }],
                links: Vec::new(),
                dlq: None,
            }],
            store: None,
            http: HttpConfig {
                bind: "0.0.0.0:8080".to_string(),
                disabled: false,
                control: true,
            },
            observability: ObservabilityConfig {
                log_format: LogFormat::Json,
                log_level: "debug".to_string(),
                otlp_endpoint: None,
                sample_ratio: 1.0,
                error_sample_ratio: 1.0,
                inspector: crate::inspector::InspectorConfig::default(),
            },
            variables: HashMap::new(),
        }
    }

    // The canonical config literal must deserialise into the same shape
    // `full_config()` builds by hand. Serialization is not exercised: nothing
    // writes a config file, and `#[serde(flatten)]` plus internally tagged
    // enums make the `Serialize` direction meaningless here. This config is
    // never `.validate()`d: the source and sink are deliberately unlinked, to
    // pin down the parse shape without also asserting the graph rules.
    #[test]
    fn test_full_standalone_config_deserialises() {
        let raw = r#"
mode "standalone"

node id=1 name="node-1" data_dir="/tmp/saci"

run_mode kind="interval" interval_ms=5000

workflow "payments" {
    source "kafka_in" type="MongoSource" component="orders"

    sink "pg_out" type="PostgresSink" component="orders"
}

http bind="0.0.0.0:8080" disabled=#false

observability log_format="json" log_level="debug"
"#;
        let restored = parse(raw).expect("deserialize");
        let original = full_config();

        assert_eq!(restored.node.id, original.node.id);
        assert_eq!(restored.node.name, original.node.name);
        assert_eq!(restored.node.data_dir, original.node.data_dir);
        assert_eq!(restored.workflows[0].id, "payments");
        assert!(
            restored.workflows[0].wasm.is_empty(),
            "no wasm node means no wasm processors"
        );
        assert_eq!(restored.workflows[0].sources.len(), 1);
        assert_eq!(restored.workflows[0].sources[0].component, "orders");
        assert_eq!(restored.workflows[0].sinks.len(), 1);
        assert_eq!(restored.workflows[0].sinks[0].component, "orders");
        assert_eq!(restored.http.bind, "0.0.0.0:8080");
        assert!(!restored.http.disabled);
        assert_eq!(restored.observability.log_level, "debug");
        assert_eq!(restored.observability.log_format, LogFormat::Json);
        match restored.mode {
            ServiceMode::Standalone { config } => {
                assert_eq!(config.run_mode, RunMode::Interval { interval_ms: 5_000 });
            }
            _ => panic!("expected standalone"),
        }
    }

    #[test]
    fn test_minimal_standalone_parses_and_validates() {
        let cfg = parse(&minimal_standalone_kdl()).expect("parse");

        assert_eq!(cfg.node.id, 1);
        assert!(cfg.node.name.is_none());
        assert_eq!(cfg.node.data_dir, PathBuf::from("/tmp/saci-test"));

        assert!(matches!(cfg.mode, ServiceMode::Standalone { .. }));

        assert_eq!(cfg.workflows[0].id, "w");
        assert_eq!(cfg.workflows[0].sources.len(), 1);
        assert_eq!(cfg.workflows[0].sinks.len(), 1);
        assert_eq!(cfg.http.bind, "0.0.0.0:8080");
        assert!(!cfg.http.disabled);
        assert_eq!(cfg.observability.log_level, "error");
        assert_eq!(cfg.observability.log_format, LogFormat::Pretty);
        cfg.validate().expect("trivial source-to-sink workflow");
    }

    /// A sampling ratio outside `0.0..=1.0` fails validation naming its own
    /// key, so an operator sees which of the two is wrong.
    #[test]
    fn test_out_of_range_sampling_ratios_are_rejected() {
        let base = minimal_standalone_kdl();

        let too_large = parse(&format!("{base}\nobservability sample_ratio=1.5\n")).expect("parse");
        let err = too_large.validate().expect_err("1.5 is not a ratio");
        assert!(
            err.message().contains("observability.sample_ratio"),
            "error should name the key: {err}"
        );

        let negative =
            parse(&format!("{base}\nobservability error_sample_ratio=-0.1\n")).expect("parse");
        let err = negative.validate().expect_err("-0.1 is not a ratio");
        assert!(
            err.message().contains("observability.error_sample_ratio"),
            "error should name the key: {err}"
        );
    }

    #[test]
    fn test_missing_workflow_node_is_a_parse_error() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"
"#;
        let err = parse(raw).expect_err("a config with no workflow node must not parse");
        assert!(
            err.contains("workflow"),
            "error should name the missing field: {err}"
        );
    }

    /// A `store "redb"` block parses into [`StoreConfig::Redb`] with
    /// `batch_resume` defaulting to false.
    #[test]
    fn test_store_redb_parses_and_validates() {
        let raw = format!(
            r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

store "redb" {{
    path "/var/lib/saci/state.redb"
}}
{TRIVIAL_WORKFLOW}
"#
        );
        let cfg = parse(&raw).expect("parse");
        match &cfg.store {
            Some(StoreConfig::Redb { path, batch_resume }) => {
                assert_eq!(path, &PathBuf::from("/var/lib/saci/state.redb"));
                assert!(!batch_resume, "batch resume is opt-in, default false");
            }
            other => panic!("expected a redb store config, got {other:?}"),
        }
        cfg.validate()
            .expect("a well-formed redb store should validate");
    }

    /// `batch_resume` is read off the block when declared.
    #[test]
    fn test_store_redb_batch_resume_opt_in() {
        let raw = format!(
            r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

store "redb" {{
    path "state.redb"
    batch_resume #true
}}
{TRIVIAL_WORKFLOW}
"#
        );
        let cfg = parse(&raw).expect("parse");
        assert!(matches!(
            cfg.store,
            Some(StoreConfig::Redb {
                batch_resume: true,
                ..
            })
        ));
    }

    /// Only `redb` is a known store kind, so any other tag is a parse error
    /// rather than a store the service silently ignores.
    #[test]
    fn test_store_unknown_kind_is_a_parse_error() {
        let raw = format!(
            r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

store "sqlite" {{
    path "state.redb"
}}
{TRIVIAL_WORKFLOW}
"#
        );
        let err = parse(&raw).expect_err("an unknown store kind must not parse");
        assert!(
            err.contains("unknown store kind"),
            "error should name the kind: {err}"
        );
    }

    /// `path` is required: a redb store with no file has nowhere to persist.
    #[test]
    fn test_store_redb_requires_path() {
        let raw = format!(
            r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

store "redb" {{
    batch_resume #true
}}
{TRIVIAL_WORKFLOW}
"#
        );
        let err = parse(&raw).expect_err("a redb store without a path must not parse");
        assert!(err.contains("path"), "error should name the field: {err}");
    }

    /// A declared but empty `path` parses and is caught by `validate`, which
    /// is the only place that can tell an empty string from an absent key.
    #[test]
    fn test_store_redb_rejects_empty_path() {
        let raw = format!(
            r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

store "redb" {{
    path ""
}}
{TRIVIAL_WORKFLOW}
"#
        );
        let cfg = parse(&raw).expect("an empty path still parses");
        let err = cfg
            .validate()
            .expect_err("an empty store path must not validate");
        assert!(
            err.to_string()
                .contains("store redb: path must not be empty"),
            "error should name the empty path: {err}"
        );
    }

    #[test]
    fn test_minimal_cluster_parses() {
        let cfg = parse(&minimal_cluster_kdl()).expect("parse");

        match &cfg.mode {
            ServiceMode::Cluster { config } => {
                assert_eq!(config.peers.len(), 2);
                assert!(config.bootstrap);
                assert_eq!(config.election_timeout_ms, default_election_timeout());
                assert_eq!(config.heartbeat_interval_ms, default_heartbeat_interval());
                assert_eq!(
                    config.snapshot_log_interval,
                    default_snapshot_log_interval()
                );
            }
            _ => panic!("expected cluster mode"),
        }
    }

    /// One `peer` node is a single table, not a list, so `ClusterConfig.peers`
    /// carries `one_or_many`.
    #[test]
    fn test_single_peer_node_parses_as_a_one_element_list() {
        let raw = format!(
            r#"
mode "cluster"

node id=1 data_dir="/tmp/saci"

peer id=1 addr="127.0.0.1:9000"
{TRIVIAL_WORKFLOW}
"#
        );
        let cfg = parse(&raw).expect("parse");
        match &cfg.mode {
            ServiceMode::Cluster { config } => {
                assert_eq!(config.peers.len(), 1);
                assert_eq!(config.peers[0].addr, "127.0.0.1:9000");
            }
            _ => panic!("expected cluster mode"),
        }
    }

    #[test]
    fn test_missing_node_id_produces_error() {
        let raw = r#"
mode "standalone"

node data_dir="/tmp/saci"
"#;
        let err = parse(raw).expect_err("expected parse error for missing node.id");
        assert!(
            err.contains("id") || err.contains("missing field"),
            "error should mention missing field: {err}"
        );
    }

    #[test]
    fn test_invalid_mode_produces_error() {
        let raw = r#"
mode "turbo_mode"

node id=1 data_dir="/tmp/saci"
"#;
        assert!(parse(raw).is_err(), "expected parse error for unknown mode");
    }

    #[test]
    fn test_cluster_node_not_in_peers_rejected() {
        let raw = format!(
            r#"
mode "cluster"

node id=99 data_dir="/tmp/saci"

peer id=1 addr="127.0.0.1:9000"
peer id=2 addr="127.0.0.2:9000"
{TRIVIAL_WORKFLOW}
"#
        );
        let cfg = parse(&raw).expect("parse should succeed");
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("99"),
            "error should mention node id 99: {err}"
        );
    }

    /// Cluster state lives in `node.data_dir`, so a `store` block there names
    /// a second home for the same data and is rejected.
    #[cfg(feature = "wasm")]
    #[test]
    fn test_cluster_with_store_block_rejected() {
        let raw = r#"
mode "cluster"
bootstrap #true

node id=1 data_dir="/tmp/saci"

peer id=1 addr="127.0.0.1:9000"

store "redb" {
    path "/tmp/saci-state.redb"
}

workflow "w" {
    wasm "p" module="p.wasm"
}
"#;
        let cfg = parse(raw).expect("parse should succeed");
        let err = cfg
            .validate()
            .expect_err("cluster mode must refuse a store block");
        assert!(
            err.to_string().contains("does not take a `store` block"),
            "error should name the rejected block: {err}"
        );
    }

    /// A cluster workflow declares no source node, so there is no admission
    /// for a controller to govern and the cluster runner builds no plan. The
    /// block is refused rather than silently ignored.
    #[cfg(feature = "wasm")]
    #[test]
    fn test_cluster_with_flow_control_block_rejected() {
        let raw = r#"
mode "cluster"
bootstrap #true

node id=1 data_dir="/tmp/saci"

peer id=1 addr="127.0.0.1:9000"

flow_control {
    start_rows 8192
}

workflow "w" {
    wasm "p" module="p.wasm"
}
"#;
        let cfg = parse(raw).expect("parse should succeed");
        let err = cfg
            .validate()
            .expect_err("cluster mode must refuse a flow_control block");
        assert!(
            err.to_string()
                .contains("does not take a `flow_control` block"),
            "error should name the rejected block: {err}"
        );
    }

    /// The refusal outranks the block's own value checks, the way the `store`
    /// refusal outranks `store redb: path must not be empty`. An operator who
    /// put the block somewhere it does nothing needs to hear that first; the
    /// range of a key inside it is beside the point.
    #[cfg(feature = "wasm")]
    #[test]
    fn test_cluster_flow_control_refusal_outranks_its_value_checks() {
        let raw = r#"
mode "cluster"
bootstrap #true

node id=1 data_dir="/tmp/saci"

peer id=1 addr="127.0.0.1:9000"

flow_control {
    growth_factor 0.5
}

workflow "w" {
    wasm "p" module="p.wasm"
}
"#;
        let cfg = parse(raw).expect("parse should succeed");
        let err = cfg
            .validate()
            .expect_err("cluster mode must refuse a flow_control block")
            .to_string();
        assert!(
            err.contains("does not take a `flow_control` block"),
            "the placement refusal must win over the range check: {err}"
        );
        assert!(
            !err.contains("growth_factor"),
            "an out-of-range key inside a block that does not belong is not the error to report: {err}"
        );
    }

    /// The same config without one validates.
    #[cfg(feature = "wasm")]
    #[test]
    fn test_cluster_without_store_validates() {
        let raw = r#"
mode "cluster"
bootstrap #true

node id=1 data_dir="/tmp/saci"

peer id=1 addr="127.0.0.1:9000"

workflow "w" {
    wasm "p" module="p.wasm"
}
"#;
        let cfg = parse(raw).expect("parse should succeed");
        cfg.validate()
            .expect("a cluster config with no store block validates");
    }

    /// A batch lease has to outlive one election, or a leader change alone
    /// would let a second node claim a range still being processed.
    #[test]
    fn test_cluster_insufficient_lease_ttl_rejected() {
        let raw = format!(
            r#"
mode "cluster"
lease_ttl_ms 1000
election_timeout_ms 1000

node id=1 data_dir="/tmp/saci"

peer id=1 addr="127.0.0.1:9000"
{TRIVIAL_WORKFLOW}
"#
        );
        let cfg = parse(&raw).expect("parse should succeed");
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("lease_ttl_ms"),
            "error should mention lease_ttl_ms: {err}"
        );
    }

    #[test]
    fn test_load_from_disk() {
        let mut file = NamedTempFile::new().expect("tempfile");
        file.write_all(minimal_standalone_kdl().as_bytes())
            .expect("write");
        let path = file.path().to_path_buf();

        let cfg = ServiceConfig::load(&path).expect("load");
        assert_eq!(cfg.node.id, 1);
        assert!(matches!(cfg.mode, ServiceMode::Standalone { .. }));
    }

    #[test]
    fn test_load_rejects_a_malformed_document_naming_the_position() {
        let mut file = NamedTempFile::new().expect("tempfile");
        file.write_all(b"mode \"standalone\nnode id=1\n")
            .expect("write");

        let err = ServiceConfig::load(file.path()).expect_err("unterminated string");
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().starts_with("parsing KDL: 1:"),
            "error should name the position: {err}"
        );
    }

    #[test]
    fn test_run_mode_interval_round_trip() {
        let raw = r#"
kind "interval"
interval_ms 3000
"#;
        let restored: RunMode = parse_as(raw).expect("deserialize");
        assert_eq!(restored, RunMode::Interval { interval_ms: 3_000 });
    }

    #[test]
    fn test_http_disabled_skips_bind_validation() {
        let raw = format!(
            r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

http bind="not-a-socket-addr" disabled=#true
{TRIVIAL_WORKFLOW}
"#
        );
        let cfg = parse(&raw).expect("parse");
        cfg.validate()
            .expect("disabled http should not validate bind");
    }

    #[test]
    fn test_cluster_mode_with_a_source_rejected_at_validate() {
        let raw = r#"
mode "cluster"

node id=1 data_dir="/tmp/saci"

peer id=1 addr="127.0.0.1:9000"
peer id=2 addr="127.0.0.2:9000"

workflow "w" {
    source "kafka_in" type="MongoSource" component="orders"
    sink "out" type="NoopSink" component="orders"
    link from="kafka_in" to="out"
}
"#;
        let cfg = parse(raw).expect("parse should succeed");
        let err = cfg.validate().unwrap_err();
        assert_eq!(
            err.category(),
            "configuration",
            "expected configuration error: {err}"
        );
        assert!(
            err.to_string()
                .contains("cluster mode runs exactly one 'wasm' or 'plugin' node"),
            "error should name the cluster rule: {err}"
        );
    }

    #[test]
    fn test_run_mode_stream_parses() {
        let restored: RunMode = parse_as("kind \"stream\"\n").expect("deserialize");
        assert_eq!(restored, RunMode::Stream);
    }

    fn stream_kdl(sources: &str, sinks: &str, links: &str) -> String {
        format!(
            r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

run_mode kind="stream"

workflow "w" {{
{sources}
{sinks}
{links}
}}
"#
        )
    }

    #[test]
    fn test_stream_mode_with_one_source_validates() {
        let raw = stream_kdl(
            r#"source "ticks" type="tcp" component="Tick""#,
            r#"sink "out" type="NoopSink" component="Tick""#,
            r#"link from="ticks" to="out""#,
        );
        let cfg = parse(&raw).expect("parse");
        match &cfg.mode {
            ServiceMode::Standalone { config } => assert_eq!(config.run_mode, RunMode::Stream),
            _ => panic!("expected standalone"),
        }
        cfg.validate().expect("one source + stream mode is valid");
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn test_stream_mode_requires_at_least_one_source() {
        // A processor-only workflow passes every earlier rule, so rule 11 is
        // what fires: stream mode needs at least one source to pull from.
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

run_mode kind="stream"

workflow "w" {
    wasm "p" module="p.wasm"
}
"#;
        let cfg = parse(raw).expect("parse");
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(
            err.to_string()
                .contains("stream run mode requires at least one 'source' node"),
            "got: {err}"
        );
    }

    #[test]
    fn test_stream_mode_with_two_sources_is_valid() {
        // Two live sources feeding the same sink: the stream runner pulls
        // them round-robin, one batch per item.
        let raw = stream_kdl(
            r#"
source "a" type="tcp" component="Tick"
source "b" type="tcp" component="Tick"
"#,
            r#"sink "out" type="NoopSink" component="Tick""#,
            r#"
link from="a" to="out"
link from="b" to="out"
"#,
        );
        let cfg = parse(&raw).expect("parse");
        cfg.validate().expect("two sources + stream mode is valid");
    }

    #[test]
    fn test_live_sources_rejected_outside_stream_mode() {
        for type_name in ["tcp", "saci"] {
            let raw = format!(
                r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {{
    source "ticks" type="{type_name}" component="Tick"
    sink "out" type="NoopSink" component="Tick"
    link from="ticks" to="out"
}}
"#
            );
            let cfg = parse(&raw).expect("parse");
            let err = cfg.validate().unwrap_err();
            assert_eq!(err.category(), "configuration", "got: {err}");
            assert!(err.to_string().contains("never reaches EOF"), "got: {err}");
        }
    }

    #[test]
    fn test_kafka_source_rejected_outside_stream_mode_unless_stop_at_end() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    source "orders_in" type="KafkaSource" component="Order"
    sink "out" type="NoopSink" component="Order"
    link from="orders_in" to="out"
}
"#;
        let cfg = parse(raw).expect("parse");
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.to_string().contains("never reaches EOF"), "got: {err}");

        let raw_stop_at_end = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    source "orders_in" type="KafkaSource" component="Order" {
        config stop_at_end=#true
    }
    sink "out" type="NoopSink" component="Order"
    link from="orders_in" to="out"
}
"#;
        let cfg = parse(raw_stop_at_end).expect("parse");
        cfg.validate()
            .expect("stop_at_end=#true makes a KafkaSource usable outside stream mode");
    }

    #[test]
    fn test_kafka_source_compacted_is_accepted_outside_stream_mode_without_stop_at_end() {
        // A compacted KafkaSource is one-shot regardless of stop_at_end: it
        // always reaches EOF once its snapshot is read, so it needs no
        // stream mode either.
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    source "orders_in" type="KafkaSource" component="Order" {
        config {
            compacted #true
            key_field "id"
        }
    }
    sink "out" type="NoopSink" component="Order"
    link from="orders_in" to="out"
}
"#;
        let cfg = parse(raw).expect("parse");
        cfg.validate()
            .expect("compacted=#true makes a KafkaSource usable outside stream mode");
    }

    #[test]
    fn test_workflow_systems_key_is_rejected() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    systems "proc" type="Proc"
}
"#;
        let err = parse(raw).expect_err("a systems node must be rejected, not silently dropped");
        assert!(
            err.contains("systems"),
            "error should name the offending key: {err}"
        );
    }

    #[test]
    fn test_workflow_components_key_is_rejected() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    components "orders" type="GenericComponent"
}
"#;
        let err = parse(raw).expect_err("a components node must be rejected, not silently dropped");
        assert!(err.contains("components"), "{err}");
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn test_wasm_watch_key_is_rejected() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    wasm "p" module="pipeline.wasm" watch=#true
}
"#;
        let err = parse(raw).expect_err("watch was never implemented; it must not parse");
        assert!(err.contains("watch"), "{err}");
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn test_wasm_only_workflow_is_a_valid_entry_point() {
        let cfg = ServiceConfig {
            heal: Default::default(),
            flow_control: Default::default(),
            node: NodeConfig {
                id: 1,
                name: None,
                data_dir: PathBuf::from("/tmp/saci"),
            },
            mode: ServiceMode::Standalone {
                config: StandaloneConfig::default(),
            },
            workflows: vec![WorkflowSpec {
                id: "w".to_string(),
                name: None,
                transformers: Vec::new(),
                sources: Vec::new(),
                wasm: vec![WasmSpec {
                    id: "p".to_string(),
                    name: None,
                    module: Some("pipeline.wasm".to_string()),
                    sha3_256: None,
                    config: HashMap::new(),
                    window: None,
                }],
                plugin: Vec::new(),
                sinks: Vec::new(),
                links: Vec::new(),
                dlq: None,
            }],
            http: HttpConfig::default(),
            store: None,
            observability: ObservabilityConfig::default(),
            variables: HashMap::new(),
        };
        cfg.validate()
            .expect("a lone processor entry point should be valid");
    }

    /// A `wasm` node parses in a binary with no wasm host, and the refusal
    /// names the feature.
    ///
    /// Runs only where the host is absent, which is the whole point: with
    /// `wasm` on there is nothing to refuse. `--all-features` skips it, so
    /// `cargo nextest run -p saci-service --no-default-features --features
    /// service --lib` is what exercises it; the wording itself is pinned in
    /// every build by
    /// `factories::tests::every_host_refusal_names_the_feature_that_supplies_it`.
    #[cfg(not(feature = "wasm"))]
    #[test]
    fn test_wasm_node_without_the_host_parses_and_names_the_feature() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    wasm "transform" module="transform.wasm"
}
"#;
        let cfg = parse(raw).expect("a wasm node must parse whether or not the host is built in");
        assert_eq!(cfg.workflows[0].wasm.len(), 1);

        let message = crate::service::validation::validate_build_capabilities(&cfg)
            .expect_err("a binary with no wasm host must refuse a wasm node")
            .message()
            .to_string();
        assert!(
            message.contains("--features wasm"),
            "the refusal must name the flag that would run it: {message}"
        );
    }

    /// The same for a `plugin` node, which is the case the default bundle
    /// hits: `plugin` is not in `default`, so this runs under
    /// `cargo nextest run --workspace --lib` too.
    #[cfg(not(feature = "plugin"))]
    #[test]
    fn test_plugin_node_without_the_host_parses_and_names_the_feature() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    plugin "audit" library="libaudit.so"
}
"#;
        let cfg = parse(raw).expect("a plugin node must parse whether or not the host is built in");
        assert_eq!(cfg.workflows[0].plugin.len(), 1);

        let message = crate::service::validation::validate_build_capabilities(&cfg)
            .expect_err("a binary with no plugin host must refuse a plugin node")
            .message()
            .to_string();
        assert!(
            message.contains("--features plugin"),
            "the refusal must name the flag that would run it: {message}"
        );
    }

    /// `mode "cluster"` in a binary with no Raft stack is refused by the same
    /// gate, so `validate` cannot report OK on a config only a cluster build
    /// can run.
    #[cfg(not(feature = "service-cluster"))]
    #[test]
    fn test_cluster_mode_without_the_raft_stack_names_the_feature() {
        let cfg = parse(&minimal_cluster_kdl()).expect("a cluster config parses in every build");

        let message = crate::service::validation::validate_build_capabilities(&cfg)
            .expect_err("a binary with no Raft stack must refuse mode cluster")
            .message()
            .to_string();
        assert!(
            message.contains("--features service-cluster"),
            "the refusal must name the flag that would run it: {message}"
        );
    }

    /// A `window` block on a `wasm` node this build *can* host, in a build
    /// with no windowing engine, is refused by the same gate.
    ///
    /// Needs `wasm`, because the missing host is reported first and would
    /// mask this one. `cargo nextest run -p saci-service
    /// --no-default-features --features service,wasm --lib` is what
    /// exercises it.
    #[cfg(all(not(feature = "windows"), feature = "wasm"))]
    #[test]
    fn test_window_block_without_the_engine_parses_and_names_the_feature() {
        let raw = workflow_kdl(
            r#"
wasm "aggregate" module="aggregate.wasm" {
    window kind="tumbling" size_ms=30000 time_field="timestamp_ms"
}
"#,
        );
        let cfg = parse(&raw).expect("a window block must parse whether or not the engine is in");
        assert!(cfg.workflows[0].wasm[0].window.is_some());

        let message = crate::service::validation::validate_build_capabilities(&cfg)
            .expect_err("a binary with no windowing engine must refuse a window block")
            .message()
            .to_string();
        assert!(
            message.contains("--features windows"),
            "the refusal must name the flag that would run it: {message}"
        );
    }

    /// The same for a `window` block on a `plugin` node.
    #[cfg(all(not(feature = "windows"), feature = "plugin"))]
    #[test]
    fn test_plugin_window_block_without_the_engine_names_the_feature() {
        let raw = workflow_kdl(
            r#"
plugin "aggregate" library="libaggregate.so" {
    window kind="session" gap_ms=10000 time_field="timestamp_ms"
}
"#,
        );
        let cfg = parse(&raw).expect("a window block must parse whether or not the engine is in");
        assert!(cfg.workflows[0].plugin[0].window.is_some());

        let message = crate::service::validation::validate_build_capabilities(&cfg)
            .expect_err("a binary with no windowing engine must refuse a window block")
            .message()
            .to_string();
        assert!(
            message.contains("--features windows"),
            "the refusal must name the flag that would run it: {message}"
        );
    }

    /// The mirror image, and the one the suite's own build reaches: a binary
    /// that *has* the hosts admits the configs that name them.
    ///
    /// The negative tests above cannot catch an inverted `cfg!` in
    /// `validate_build_capabilities`: reading `cfg!(feature = "wasm")`
    /// instead of its negation would make every default binary refuse every
    /// wasm config, and nothing else in the suite calls the gate, since it is
    /// reached only from the binary.
    #[cfg(all(feature = "wasm", feature = "plugin"))]
    #[test]
    fn test_declared_hosts_this_build_carries_are_admitted() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    wasm "transform" module="transform.wasm"
    plugin "audit" library="libaudit.so"
    link from="transform" to="audit"
}
"#;
        let cfg = parse(raw).expect("parse");
        crate::service::validation::validate_build_capabilities(&cfg)
            .expect("a build carrying both hosts must admit both node kinds");
    }

    /// The same for `mode "cluster"` in a build that carries the Raft stack.
    #[cfg(feature = "service-cluster")]
    #[test]
    fn test_cluster_mode_is_admitted_by_a_cluster_build() {
        let cfg = parse(&minimal_cluster_kdl()).expect("parse");
        crate::service::validation::validate_build_capabilities(&cfg)
            .expect("a cluster build must admit mode cluster");
    }

    /// The same for a `window` block in a build that carries the engine,
    /// which is what catches an inverted `cfg!(feature = "windows")`: that
    /// mutation would make every default binary refuse every windowed
    /// config, and the negative test above cannot see it.
    #[cfg(all(feature = "windows", feature = "wasm"))]
    #[test]
    fn test_a_window_block_is_admitted_by_a_windowing_build() {
        let raw = workflow_kdl(
            r#"
wasm "aggregate" module="aggregate.wasm" {
    window kind="tumbling" size_ms=30000 time_field="timestamp_ms"
}
"#,
        );
        let cfg = parse(&raw).expect("parse");
        crate::service::validation::validate_build_capabilities(&cfg)
            .expect("a windowing build must admit a window block");
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn test_two_wasm_nodes_are_independent_processors_linked_explicitly() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    wasm "validate" module="validate.wasm" {
        config min_amount="0.50"
    }
    wasm "settle" module="settle.wasm" {
        config fee_bps="290"
    }
    link from="validate" to="settle"
}
"#;
        let cfg = parse(raw).expect("parse");
        assert_eq!(cfg.workflows[0].wasm.len(), 2);
        assert_eq!(cfg.workflows[0].wasm[0].id, "validate");
        assert_eq!(
            cfg.workflows[0].wasm[0].module.as_deref(),
            Some("validate.wasm")
        );
        assert_eq!(
            cfg.workflows[0].wasm[0]
                .config
                .get("min_amount")
                .map(String::as_str),
            Some("0.50")
        );
        assert_eq!(cfg.workflows[0].wasm[1].id, "settle");
        assert_eq!(
            cfg.workflows[0].wasm[1]
                .config
                .get("fee_bps")
                .map(String::as_str),
            Some("290")
        );
        assert_eq!(
            cfg.workflows[0].links,
            vec![LinkSpec {
                from: "validate".to_string(),
                to: "settle".to_string(),
                branch: None,
            }]
        );
        cfg.validate().expect("two linked processors are valid");
    }

    /// `one_or_many` goes through the value tree rather than an untagged enum
    /// specifically so a `deny_unknown_fields` violation inside one entry
    /// still names the offending key instead of collapsing into a generic
    /// "data did not match any variant" error.
    #[cfg(feature = "wasm")]
    #[test]
    fn test_wasm_unknown_key_is_rejected_with_field_name() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    wasm "validate" module="validate.wasm"
    wasm "settle" module="settle.wasm" watch=#true
}
"#;
        let err = parse(raw).expect_err("watch was never implemented; it must not parse");
        assert!(err.contains("watch"), "{err}");
    }

    #[cfg(feature = "plugin")]
    #[test]
    fn test_plugin_workflow_round_trips() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    plugin "audit" library="pipelines/libtransform.so" sha3_256="sha3-256:abc123" {
        config "smoketest.multiplier"="10"
    }
}
"#;
        let cfg = parse(raw).expect("parse");
        let spec = cfg.workflows[0]
            .plugin
            .first()
            .expect("the plugin node should parse");
        assert_eq!(spec.id, "audit");
        assert_eq!(spec.library.as_deref(), Some("pipelines/libtransform.so"));
        assert_eq!(spec.sha3_256.as_deref(), Some("sha3-256:abc123"));
        assert_eq!(
            spec.config.get("smoketest.multiplier").map(String::as_str),
            Some("10")
        );
        cfg.validate()
            .expect("plugin-only workflow should be valid");
    }

    #[cfg(feature = "plugin")]
    #[test]
    fn test_plugin_unknown_key_is_rejected() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    plugin "p" library="libtransform.so" module="libtransform.so"
}
"#;
        let err = parse(raw).expect_err("a key the loader cannot honour must not parse");
        assert!(err.contains("module"), "{err}");
    }

    #[cfg(feature = "plugin")]
    #[test]
    fn test_plugin_digest_and_config_default_to_empty() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    plugin "p" library="libtransform.so"
}
"#;
        let cfg = parse(raw).expect("parse");
        let spec = cfg
            .workflows
            .into_iter()
            .next()
            .expect("a workflow")
            .plugin
            .into_iter()
            .next()
            .expect("the plugin node should parse");
        assert!(spec.name.is_none(), "name is optional");
        assert!(spec.sha3_256.is_none(), "digest is optional");
        assert!(spec.config.is_empty(), "config defaults to an empty table");
    }

    #[cfg(feature = "plugin")]
    #[test]
    fn test_plugin_with_no_library_relies_on_a_registered_native_runtime() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    plugin "p" name="P"
}
"#;
        let cfg = parse(raw).expect("parse");
        let spec = &cfg.workflows[0].plugin[0];
        assert!(spec.library.is_none());
        cfg.validate()
            .expect("an artifact-less processor entry point is structurally valid");
    }

    /// A misspelled node kind is still a parse error naming the key.
    ///
    /// This is the half of [`WorkflowSpec`]'s `deny_unknown_fields` that the
    /// unconditional `wasm`/`plugin` fields put at risk. Declaring a host this
    /// build does not carry is deliberately not an unknown key, so
    /// that `validate_build_capabilities` can name the feature
    /// (`test_plugin_node_without_the_host_parses_and_names_the_feature`); a
    /// genuine typo must still be refused by name, which is what
    /// `docs/content/service/operate/troubleshooting.md`'s "A key it does not
    /// know" promises. Ungated: both properties hold in every build.
    ///
    /// Asserted on the offending key alone. Serde's `expected one of ...`
    /// tail is derive-generated, so its ordering and punctuation are
    /// implementation details no config file depends on.
    #[test]
    fn test_unknown_workflow_key_is_rejected_and_names_it() {
        let raw = workflow_kdl(r#"    wasmm "transform" module="transform.wasm""#);
        let err = parse(&raw).expect_err("a misspelled node kind must not parse");
        assert!(
            err.contains("wasmm"),
            "the error must name the offending key: {err}"
        );
    }

    /// The same inside a `source`, which has its own, different valid set.
    ///
    /// A workflow-level check cannot stand in for this one: the two structs
    /// carry `deny_unknown_fields` independently, and a node body is where a
    /// key is most easily mistaken for one a connector would read. Only a
    /// key inside the nested `config` block belongs to the connector.
    #[test]
    fn test_unknown_source_key_is_rejected_and_names_it() {
        let raw =
            workflow_kdl(r#"    source "in" type="FileSource" component="orders" truncat=#true"#);
        let err = parse(&raw).expect_err("a key no source can honour must not parse");
        assert!(
            err.contains("truncat"),
            "the error must name the offending key: {err}"
        );
    }

    #[test]
    fn test_standalone_mode_with_a_linked_source_and_sink_is_valid() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    source "kafka_in" type="MongoSource" component="orders"
    sink "out" type="NoopSink" component="orders"
    link from="kafka_in" to="out"
}
"#;
        let cfg = parse(raw).expect("parse should succeed");
        cfg.validate()
            .expect("standalone mode with a linked source and sink should be valid");
    }

    // ── WorkflowSpec::validate: graph rules ─────────────────────────────────

    fn workflow_kdl(body: &str) -> String {
        format!(
            r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {{
{body}
}}
"#
        )
    }

    fn workflow_err(body: &str) -> String {
        let cfg = parse(&workflow_kdl(body)).expect("parse should succeed");
        cfg.validate()
            .expect_err("expected a graph validation error")
            .to_string()
    }

    #[test]
    fn rule_id_charset_is_enforced() {
        let err = workflow_err(
            r#"
source "bad/id" type="NoopSource" component="X"
sink "out" type="NoopSink" component="X"
link from="bad/id" to="out"
"#,
        );
        assert!(err.contains("is invalid"), "got: {err}");
        assert!(err.contains("bad/id"), "got: {err}");
    }

    #[test]
    fn rule_duplicate_id_across_kinds_is_rejected() {
        let err = workflow_err(
            r#"
source "shared" type="NoopSource" component="X"
sink "shared" type="NoopSink" component="X"
link from="shared" to="shared"
"#,
        );
        assert!(err.contains("declared twice"), "got: {err}");
    }

    #[test]
    fn rule_source_transformer_must_be_declared() {
        let err = workflow_err(
            r#"
source "in" type="NoopSource" transformer="missing" component="X"
sink "out" type="NoopSink" component="X"
link from="in" to="out"
"#,
        );
        assert!(err.contains("names transformer 'missing'"), "got: {err}");
    }

    #[test]
    fn rule_link_to_undeclared_node_is_rejected() {
        let err = workflow_err(
            r#"
source "in" type="NoopSource" component="X"
sink "out" type="NoopSink" component="X"
link from="in" to="ghost"
"#,
        );
        assert!(err.contains("undeclared node 'ghost'"), "got: {err}");
    }

    #[test]
    fn rule_self_link_is_rejected() {
        let err = workflow_err(
            r#"
source "in" type="NoopSource" component="X"
link from="in" to="in"
"#,
        );
        assert!(err.contains("links a node to itself"), "got: {err}");
    }

    #[test]
    fn rule_duplicate_link_is_rejected() {
        let err = workflow_err(
            r#"
source "in" type="NoopSource" component="X"
sink "out" type="NoopSink" component="X"
link from="in" to="out"
link from="in" to="out"
"#,
        );
        assert!(err.contains("is declared twice"), "got: {err}");
    }

    #[test]
    fn rule_link_into_a_source_is_rejected() {
        let err = workflow_err(
            r#"
source "a" type="NoopSource" component="X"
source "b" type="NoopSource" component="X"
link from="a" to="b"
"#,
        );
        assert!(err.contains("a source has no input"), "got: {err}");
    }

    #[test]
    fn rule_link_out_of_a_sink_is_rejected() {
        let err = workflow_err(
            r#"
source "in" type="NoopSource" component="X"
sink "a" type="NoopSink" component="X"
sink "b" type="NoopSink" component="X"
link from="in" to="a"
link from="a" to="b"
"#,
        );
        assert!(err.contains("a sink has no output"), "got: {err}");
    }

    #[test]
    fn rule_two_link_cycle_is_rejected() {
        #[cfg(feature = "wasm")]
        {
            let err = workflow_err(
                r#"
wasm "p1" module="p1.wasm"
wasm "p2" module="p2.wasm"
link from="p1" to="p2"
link from="p2" to="p1"
"#,
            );
            assert!(err.contains("links contain a cycle"), "got: {err}");
        }
    }

    #[test]
    fn rule_source_with_no_outbound_link_is_rejected() {
        let err = workflow_err(r#"source "in" type="NoopSource" component="X""#);
        assert!(
            err.contains("source 'in' has no outbound link"),
            "got: {err}"
        );
    }

    #[test]
    fn rule_sink_with_no_inbound_link_is_rejected() {
        let err = workflow_err(r#"sink "out" type="NoopSink" component="X""#);
        assert!(err.contains("sink 'out' has no inbound link"), "got: {err}");
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn rule_branch_charset_is_enforced() {
        let err = workflow_err(
            r#"
wasm "p" module="p.wasm"
sink "a" type="NoopSink" component="X"
sink "b" type="NoopSink" component="X"
link from="p" to="a" branch="bad/branch"
link from="p" to="b" branch="high"
"#,
        );
        assert!(err.contains("is invalid"), "got: {err}");
        assert!(err.contains("bad/branch"), "got: {err}");
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn rule_branch_on_a_source_link_is_rejected() {
        let err = workflow_err(
            r#"
source "in" type="NoopSource" component="X"
sink "out" type="NoopSink" component="X"
link from="in" to="out" branch="high"
"#,
        );
        assert!(err.contains("only a processor can route"), "got: {err}");
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn rule_node_mixes_labelled_and_unlabelled_links_is_rejected() {
        let err = workflow_err(
            r#"
wasm "p" module="p.wasm"
sink "a" type="NoopSink" component="X"
sink "b" type="NoopSink" component="X"
link from="p" to="a" branch="high"
link from="p" to="b"
"#,
        );
        assert!(err.contains("mixes labelled and unlabelled"), "got: {err}");
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn rule_two_labelled_links_validate() {
        let cfg = parse(&workflow_kdl(
            r#"
wasm "p" module="p.wasm"
sink "a" type="NoopSink" component="X"
sink "b" type="NoopSink" component="X"
link from="p" to="a" branch="high"
link from="p" to="b" branch="low"
"#,
        ))
        .expect("parse should succeed");
        cfg.validate().expect("two labelled links are valid");
    }

    // ── Cross-workflow rules: ServiceConfig::validate ────────────────────────

    #[test]
    fn test_two_workflow_blocks_parse_into_two_workflows() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "a" {
    source "in_a" type="NoopSource" component="X"
    sink "out_a" type="NoopSink" component="X"
    link from="in_a" to="out_a"
}

workflow "b" {
    source "in_b" type="NoopSource" component="Y"
    sink "out_b" type="NoopSink" component="Y"
    link from="in_b" to="out_b"
}
"#;
        let cfg = parse(raw).expect("parse");
        assert_eq!(cfg.workflows.len(), 2);
        assert_eq!(cfg.workflows[0].id, "a");
        assert_eq!(cfg.workflows[1].id, "b");
        cfg.validate()
            .expect("two independent workflows with disjoint ids validate");
    }

    #[test]
    fn rule_duplicate_node_id_across_workflows_is_rejected() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "a" {
    source "shared" type="NoopSource" component="X"
    sink "out_a" type="NoopSink" component="X"
    link from="shared" to="out_a"
}

workflow "b" {
    source "shared" type="NoopSource" component="Y"
    sink "out_b" type="NoopSink" component="Y"
    link from="shared" to="out_b"
}
"#;
        let cfg = parse(raw).expect("parse should succeed");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("node id 'shared'"), "got: {err}");
        assert!(err.contains("workflow 'a'"), "got: {err}");
        assert!(err.contains("workflow 'b'"), "got: {err}");
    }

    #[test]
    fn rule_duplicate_workflow_id_is_rejected() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "dup" {
    source "in_a" type="NoopSource" component="X"
    sink "out_a" type="NoopSink" component="X"
    link from="in_a" to="out_a"
}

workflow "dup" {
    source "in_b" type="NoopSource" component="Y"
    sink "out_b" type="NoopSink" component="Y"
    link from="in_b" to="out_b"
}
"#;
        let cfg = parse(raw).expect("parse should succeed");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("workflow id 'dup' is declared twice"),
            "got: {err}"
        );
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn rule_cluster_mode_requires_exactly_one_workflow() {
        let raw = r#"
mode "cluster"
bootstrap #true

node id=1 data_dir="/tmp/saci"

peer id=1 addr="127.0.0.1:9000"

workflow "a" {
    wasm "p1" module="p1.wasm"
}

workflow "b" {
    wasm "p2" module="p2.wasm"
}
"#;
        let cfg = parse(raw).expect("parse should succeed");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("cluster mode requires exactly one workflow; found 2"),
            "got: {err}"
        );
    }

    #[test]
    fn rule_channel_source_with_no_paired_sink_is_rejected() {
        let err = workflow_err(
            r#"
source "in" type="ChannelSource" component="X" {
    config name="orphan"
}
sink "out" type="NoopSink" component="X"
link from="in" to="out"
"#,
        );
        assert!(
            err.contains("channel 'orphan': declares a ChannelSource but no ChannelSink"),
            "got: {err}"
        );
    }

    #[test]
    fn rule_channel_sink_with_no_paired_source_is_rejected() {
        let err = workflow_err(
            r#"
source "in" type="NoopSource" component="X"
sink "out" type="ChannelSink" component="X" {
    config name="orphan"
}
link from="in" to="out"
"#,
        );
        assert!(
            err.contains("channel 'orphan': declares a ChannelSink but no ChannelSource"),
            "got: {err}"
        );
    }

    #[test]
    fn rule_duplicate_channel_source_name_is_rejected() {
        let err = workflow_err(
            r#"
source "in1" type="ChannelSource" component="X" {
    config name="dup"
}
source "in2" type="ChannelSource" component="X" {
    config name="dup"
}
sink "out1" type="NoopSink" component="X"
sink "out2" type="NoopSink" component="X"
link from="in1" to="out1"
link from="in2" to="out2"
"#,
        );
        assert!(
            err.contains("channel 'dup': more than one ChannelSource declared"),
            "got: {err}"
        );
    }

    #[test]
    fn rule_duplicate_channel_sink_name_is_rejected() {
        let err = workflow_err(
            r#"
source "in1" type="NoopSource" component="X"
source "in2" type="NoopSource" component="X"
sink "out1" type="ChannelSink" component="X" {
    config name="dup"
}
sink "out2" type="ChannelSink" component="X" {
    config name="dup"
}
link from="in1" to="out1"
link from="in2" to="out2"
"#,
        );
        assert!(
            err.contains("channel 'dup': more than one ChannelSink declared"),
            "got: {err}"
        );
    }

    #[test]
    fn two_workflows_bridged_by_a_named_channel_validate() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "producer" {
    source "in" type="NoopSource" component="X"
    sink "bridge_out" type="ChannelSink" component="X" {
        config name="bridge"
    }
    link from="in" to="bridge_out"
}

workflow "consumer" {
    source "bridge_in" type="ChannelSource" component="X" {
        config name="bridge"
    }
    sink "out" type="NoopSink" component="X"
    link from="bridge_in" to="out"
}
"#;
        let cfg = parse(raw).expect("parse should succeed");
        cfg.validate()
            .expect("a sink in one workflow paired with a source in another is valid");
    }

    // ── Windowing: the `window` block on processor nodes ────────────────────

    /// The old rule 10 rejected a processor fed by both a source and another
    /// processor. Fan-in merging is exactly what a windowed processor is for:
    /// one node receives rows from several streams and merges them, so the
    /// rule is gone and this shape must validate.
    #[cfg(feature = "wasm")]
    #[test]
    fn mixed_source_and_processor_fan_in_is_valid() {
        let cfg = parse(&workflow_kdl(
            r#"
source "s" type="NoopSource" component="X"
wasm "up" module="up.wasm"
wasm "down" module="down.wasm"
link from="s" to="down"
link from="up" to="down"
"#,
        ))
        .expect("parse should succeed");
        cfg.validate()
            .expect("a processor may be fed by sources and processors at once");
    }

    #[test]
    fn window_block_parses_with_one_and_many_key_fields() {
        let raw = workflow_kdl(
            r#"
wasm "p" module="p.wasm" {
    window kind="tumbling" size_ms=30000 offset_ms=500 time_field="timestamp_ms" allowed_lateness_ms=5000 {
        key_field "category"
        key_field "region"
    }
}
"#,
        );
        let cfg = parse(&raw).expect("parse");
        let window = cfg.workflows[0].wasm[0]
            .window
            .as_ref()
            .expect("window block");
        assert_eq!(
            window.spec,
            saci_core::window_spec::WindowSpec::Tumbling {
                size_ms: 30_000,
                offset_ms: 500,
            }
        );
        assert_eq!(window.time_field, "timestamp_ms");
        assert_eq!(window.key_fields, vec!["category", "region"]);
        assert_eq!(window.allowed_lateness_ms, 5_000);
        cfg.validate().expect("a sane window block validates");

        // A single key_field child is a scalar, not an array: the one-or-many
        // deserializer must accept both shapes.
        let raw = workflow_kdl(
            r#"
wasm "p" module="p.wasm" {
    window kind="session" gap_ms=10000 time_field="ts" {
        key_field "category"
    }
}
"#,
        );
        let cfg = parse(&raw).expect("parse");
        let window = cfg.workflows[0].wasm[0]
            .window
            .as_ref()
            .expect("window block");
        assert_eq!(
            window.spec,
            saci_core::window_spec::WindowSpec::Session { gap_ms: 10_000 }
        );
        assert_eq!(window.key_fields, vec!["category"]);
        assert_eq!(window.allowed_lateness_ms, 0, "lateness defaults to zero");
    }

    #[test]
    fn window_unknown_key_is_rejected() {
        let raw = workflow_kdl(
            r#"
wasm "p" module="p.wasm" {
    window kind="tumbling" size_ms=1000 time_field="ts" bogus=1
}
"#,
        );
        let err = parse(&raw).expect_err("an unknown window key must not parse");
        assert!(err.contains("bogus"), "got: {err}");
    }

    #[test]
    fn window_unknown_kind_is_rejected() {
        let raw = workflow_kdl(
            r#"
wasm "p" module="p.wasm" {
    window kind="hourly" size_ms=1000 time_field="ts"
}
"#,
        );
        let err = parse(&raw).expect_err("an unknown window kind must not parse");
        assert!(err.contains("hourly"), "got: {err}");
    }

    #[test]
    fn window_missing_time_field_is_rejected() {
        let raw = workflow_kdl(
            r#"
wasm "p" module="p.wasm" {
    window kind="tumbling" size_ms=1000
}
"#,
        );
        let err = parse(&raw).expect_err("time_field is mandatory");
        assert!(err.contains("time_field"), "got: {err}");
    }

    #[test]
    fn window_geometry_key_wrong_for_kind_is_rejected() {
        let raw = workflow_kdl(
            r#"
wasm "p" module="p.wasm" {
    window kind="session" gap_ms=1000 size_ms=500 time_field="ts"
}
"#,
        );
        let err = parse(&raw).expect_err("size_ms is not a session geometry");
        assert!(err.contains("size_ms"), "got: {err}");
    }

    #[test]
    fn window_invalid_geometry_is_rejected_at_validate() {
        let raw = workflow_kdl(
            r#"
wasm "p" module="p.wasm" {
    window kind="sliding" size_ms=1000 slide_ms=2000 time_field="ts"
}
"#,
        );
        let cfg = parse(&raw).expect("parse");
        let err = cfg
            .validate()
            .expect_err("slide > size is nonsense and must be rejected")
            .to_string();
        assert!(err.contains("window is invalid"), "got: {err}");
        assert!(err.contains("slide_ms"), "got: {err}");
    }

    // ------------------------------------------------------------ retry

    /// A standalone workflow whose source node carries `retry` as a child
    /// node, so a test can vary the retry block's properties.
    fn source_retry_kdl(retry: &str) -> String {
        format!(
            r#"
mode "standalone"

node id=1 data_dir="/tmp/saci-test"

workflow "w" {{
    source "in" type="NoopSource" component="X" {{
        {retry}
    }}
    sink "out" type="NoopSink" component="X"
    link from="in" to="out"
}}
"#
        )
    }

    fn assert_exponential_backoff(
        config: SystemConfig,
        max_retries: usize,
        base_delay_ms: u64,
        multiplier: f64,
        max_delay_ms: u64,
        jitter: f64,
    ) {
        match config.retry_mode {
            RetryMode::ExponentialBackoff {
                max_retries: got_retries,
                base_delay,
                multiplier: got_multiplier,
                max_delay,
                jitter: got_jitter,
            } => {
                assert_eq!(got_retries, max_retries);
                assert_eq!(base_delay, Duration::from_millis(base_delay_ms));
                assert_eq!(got_multiplier, multiplier);
                assert_eq!(max_delay, Duration::from_millis(max_delay_ms));
                assert_eq!(got_jitter, jitter);
            }
            other => panic!("expected ExponentialBackoff, got {other:?}"),
        }
    }

    #[test]
    fn retry_defaults_when_the_block_is_omitted() {
        let cfg = parse(&minimal_standalone_kdl()).expect("parse should succeed");
        let wf = &cfg.workflows[0];
        assert_eq!(wf.sources[0].retry, RetryConfig::default());
        assert_eq!(wf.sinks[0].retry, RetryConfig::default());
        assert_exponential_backoff(
            wf.sources[0].retry.to_system_config(),
            3,
            100,
            2.0,
            30_000,
            0.1,
        );
    }

    #[test]
    fn a_retry_block_parses() {
        let cfg = parse(&source_retry_kdl(
            "retry max_attempts=8 base_delay_ms=500 multiplier=2.0 max_delay_ms=10000 jitter=0.1",
        ))
        .expect("parse should succeed");
        let retry = &cfg.workflows[0].sources[0].retry;
        assert_eq!(retry.max_attempts, 8);
        assert_eq!(retry.base_delay_ms, 500);
        assert_eq!(retry.multiplier, 2.0);
        assert_eq!(retry.max_delay_ms, 10_000);
        assert_eq!(retry.jitter, 0.1);
    }

    #[test]
    fn max_attempts_one_disables_retrying() {
        let cfg = parse(&source_retry_kdl("retry max_attempts=1")).expect("parse should succeed");
        let sys = cfg.workflows[0].sources[0].retry.to_system_config();
        assert!(matches!(sys.retry_mode, RetryMode::None));
        assert_eq!(sys.retry_mode.max_attempts(), 1);
    }

    #[test]
    fn custom_params_map_to_an_exponential_backoff() {
        let cfg = parse(&source_retry_kdl(
            "retry max_attempts=8 base_delay_ms=500 multiplier=2.0 max_delay_ms=10000 jitter=0.1",
        ))
        .expect("parse should succeed");
        let sys = cfg.workflows[0].sources[0].retry.to_system_config();
        assert_exponential_backoff(sys, 7, 500, 2.0, 10_000, 0.1);
    }

    #[test]
    fn retry_max_attempts_zero_is_a_configuration_error() {
        let cfg = parse(&source_retry_kdl("retry max_attempts=0")).expect("parse should succeed");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("retry.max_attempts must be at least 1"),
            "got: {err}"
        );
        assert!(err.contains("source 'in'"), "got: {err}");
    }

    #[test]
    fn retry_multiplier_below_one_is_a_configuration_error() {
        let cfg = parse(&source_retry_kdl("retry multiplier=0.5")).expect("parse should succeed");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("retry.multiplier must be at least 1.0, got 0.5"),
            "got: {err}"
        );
    }

    #[test]
    fn retry_jitter_out_of_range_is_a_configuration_error() {
        let cfg = parse(&source_retry_kdl("retry jitter=1.5")).expect("parse should succeed");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("retry.jitter must be within 0.0..=1.0, got 1.5"),
            "got: {err}"
        );
    }

    // ── flow_control block ───────────────────────────────────────────────────

    /// The trivial workflow with `global` at the top level and `local` inside
    /// the source node, in `run_mode`.
    fn flow_kdl_mode(run_mode: &str, global: &str, local: &str) -> String {
        format!(
            r#"
mode "standalone"

node id=1 data_dir="/tmp/saci-test"

{run_mode}
{global}

workflow "w" {{
    source "in" type="NoopSource" component="X" {{
        {local}
    }}
    sink "out" type="NoopSink" component="X"
    link from="in" to="out"
}}
"#
        )
    }

    /// The same workflow in the default (continuous) run mode.
    fn flow_kdl(global: &str, local: &str) -> String {
        flow_kdl_mode("", global, local)
    }

    /// The settings a config resolves for source `in`.
    fn resolved(cfg: &ServiceConfig) -> FlowSettings {
        crate::service::flow::FlowPlan::from_config(cfg).for_source("in")
    }

    #[test]
    fn flow_control_defaults_to_the_enabled_policy_when_the_block_is_absent() {
        let cfg = parse(&minimal_standalone_kdl()).expect("parse should succeed");
        cfg.validate().expect("the default policy is valid");
        assert_eq!(cfg.flow_control, FlowControlConfig::default());
        assert_eq!(cfg.workflows[0].sources[0].flow_control, None);

        let settings = resolved(&cfg);
        assert_eq!(settings, FlowSettings::default());
        assert!(settings.enabled, "flow control is on by default");
        assert_eq!(settings.min_rows, 1_024);
        assert_eq!(settings.max_rows, 65_536);
        assert_eq!(settings.start_rows, 4_096);
        assert_eq!(settings.max_chunk_bytes, 8 * 1024 * 1024);
        assert_eq!(settings.adjust_interval_ms, 60_000);
        assert_eq!(settings.growth_factor, 2.0);
        assert_eq!(settings.improve_threshold, 0.05);
        assert_eq!(settings.min_samples_per_arm, 4);
        assert_eq!(settings.settle_after_epochs, 3);
        assert_eq!(settings.backoff_factor, 2.0);
        assert_eq!(settings.backoff_cooldown, 4);
        assert_eq!(settings.fixed_rows, None);
    }

    #[test]
    fn stream_mode_carries_a_latency_objective_and_batch_mode_does_not() {
        let stream = parse(&flow_kdl_mode(r#"run_mode kind="stream""#, "", ""))
            .expect("parse should succeed");
        assert_eq!(
            resolved(&stream).target_latency_ms,
            250,
            "a stream workflow's contract is per-item latency"
        );

        for mode in ["", r#"run_mode kind="interval" interval_ms=5000"#] {
            let batch = parse(&flow_kdl_mode(mode, "", "")).expect("parse should succeed");
            assert_eq!(
                resolved(&batch).target_latency_ms,
                0,
                "a batch pass has no latency contract, so throughput is the objective ({mode:?})"
            );
        }
    }

    #[test]
    fn an_explicit_latency_objective_wins_in_either_mode() {
        let batch = parse(&flow_kdl("flow_control { target_latency_ms 90 }", ""))
            .expect("parse should succeed");
        assert_eq!(resolved(&batch).target_latency_ms, 90);

        let stream = parse(&flow_kdl_mode(
            r#"run_mode kind="stream""#,
            "flow_control { target_latency_ms 0 }",
            "",
        ))
        .expect("parse should succeed");
        assert_eq!(
            resolved(&stream).target_latency_ms,
            0,
            "zeroing the objective in stream mode must be honoured"
        );
    }

    #[test]
    fn a_top_level_flow_control_block_parses_every_key() {
        let cfg = parse(&flow_kdl(
            r#"flow_control {
    enabled #true
    min_rows 512
    max_rows 4096
    start_rows 2048
    max_chunk_bytes 1048576
    target_latency_ms 50
    adjust_interval_ms 15000
    growth_factor 1.5
    improve_threshold 0.10
    min_samples_per_arm 8
    settle_after_epochs 6
    backoff_factor 4.0
    backoff_cooldown 2
}"#,
            "",
        ))
        .expect("parse should succeed");
        cfg.validate().expect("valid");
        assert_eq!(
            resolved(&cfg),
            FlowSettings {
                min_rows: 512,
                max_rows: 4_096,
                start_rows: 2_048,
                max_chunk_bytes: 1_048_576,
                target_latency_ms: 50,
                adjust_interval_ms: 15_000,
                growth_factor: 1.5,
                improve_threshold: 0.10,
                min_samples_per_arm: 8,
                settle_after_epochs: 6,
                backoff_factor: 4.0,
                backoff_cooldown: 2,
                fixed_rows: None,
                enabled: true,
            }
        );
    }

    #[test]
    fn a_source_block_overrides_the_global_one_field_by_field() {
        let cfg = parse(&flow_kdl(
            "flow_control { min_rows 512; max_rows 4096; adjust_interval_ms 30000 }",
            "flow_control { max_rows 2048; growth_factor 1.25 }",
        ))
        .expect("parse should succeed");
        cfg.validate().expect("valid");
        let settings = resolved(&cfg);
        assert_eq!(settings.max_rows, 2_048, "the source's own value wins");
        assert_eq!(settings.growth_factor, 1.25);
        assert_eq!(settings.min_rows, 512, "an absent field inherits");
        assert_eq!(settings.adjust_interval_ms, 30_000);
        assert_eq!(
            settings.improve_threshold, 0.05,
            "a key neither block declares takes the hard default"
        );
    }

    #[test]
    fn a_source_can_pin_a_fixed_size() {
        let cfg = parse(&flow_kdl("", "flow_control { rows 4096 }")).expect("parse should succeed");
        cfg.validate().expect("valid");
        assert_eq!(resolved(&cfg).fixed_rows, Some(4_096));
    }

    #[test]
    fn flow_control_disabled_resolves_to_a_disabled_policy() {
        let cfg =
            parse(&flow_kdl("flow_control { enabled #false }", "")).expect("parse should succeed");
        cfg.validate().expect("valid");
        assert!(!resolved(&cfg).enabled);
    }

    #[test]
    fn string_valued_flow_control_keys_parse_so_env_substitution_works() {
        let cfg = parse(&flow_kdl(
            r#"flow_control { enabled "true"; max_rows "8192"; growth_factor "1.5" }"#,
            "",
        ))
        .expect("parse should succeed");
        let settings = resolved(&cfg);
        assert!(settings.enabled);
        assert_eq!(settings.max_rows, 8_192);
        assert_eq!(settings.growth_factor, 1.5);
    }

    #[test]
    fn a_start_rows_below_the_effective_min_is_a_configuration_error() {
        let cfg = parse(&flow_kdl(
            "flow_control { min_rows 1000; max_rows 2000; start_rows 10 }",
            "",
        ))
        .expect("parse should succeed");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("start_rows (10) must be within min_rows (1000)..=max_rows (2000)"),
            "got: {err}"
        );
    }

    #[test]
    fn a_start_rows_above_the_effective_max_is_a_configuration_error() {
        let cfg = parse(&flow_kdl(
            "flow_control { min_rows 1000; max_rows 2000; start_rows 5000 }",
            "",
        ))
        .expect("parse should succeed");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("start_rows (5000) must be within min_rows (1000)..=max_rows (2000)"),
            "got: {err}"
        );
    }

    #[test]
    fn flow_control_min_rows_zero_is_a_configuration_error() {
        let cfg =
            parse(&flow_kdl("flow_control { min_rows 0 }", "")).expect("parse should succeed");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("flow_control: min_rows must be at least 1"),
            "got: {err}"
        );
    }

    #[test]
    fn flow_control_max_rows_below_min_rows_is_a_configuration_error() {
        let cfg = parse(&flow_kdl(
            "flow_control { min_rows 4096; max_rows 1024 }",
            "",
        ))
        .expect("parse should succeed");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("max_rows (1024) must be at least min_rows (4096)"),
            "got: {err}"
        );
    }

    #[test]
    fn a_ceiling_below_the_default_floor_is_a_configuration_error() {
        // Declared alone, `max_rows` is still compared against the floor that
        // takes effect, so an unusable range is refused instead of silently
        // widened.
        let cfg =
            parse(&flow_kdl("flow_control { max_rows 100 }", "")).expect("parse should succeed");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("max_rows (100) must be at least min_rows (1024)"),
            "got: {err}"
        );
    }

    #[test]
    fn flow_control_rows_zero_is_a_configuration_error() {
        let cfg = parse(&flow_kdl("flow_control { rows 0 }", "")).expect("parse should succeed");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("flow_control: rows must be at least 1"),
            "got: {err}"
        );
    }

    #[test]
    fn a_fixed_size_alongside_a_range_is_a_configuration_error() {
        let cfg = parse(&flow_kdl("flow_control { rows 4096; max_rows 8192 }", ""))
            .expect("parse should succeed");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("rows pins a fixed size and cannot be combined with"),
            "got: {err}"
        );
    }

    #[test]
    fn a_zero_adjust_interval_is_a_configuration_error_while_adaptive() {
        let cfg = parse(&flow_kdl("flow_control { adjust_interval_ms 0 }", ""))
            .expect("parse should succeed");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("adjust_interval_ms must be at least 1"),
            "got: {err}"
        );
    }

    #[test]
    fn a_zero_adjust_interval_is_accepted_when_nothing_adapts() {
        let cfg = parse(&flow_kdl(
            "flow_control { enabled #false; adjust_interval_ms 0 }",
            "",
        ))
        .expect("parse should succeed");
        cfg.validate()
            .expect("the epoch is meaningless with adaptation off");
    }

    #[test]
    fn a_growth_factor_of_one_or_less_is_a_configuration_error() {
        for value in ["1.0", "0.5"] {
            let cfg = parse(&flow_kdl(
                &format!("flow_control {{ growth_factor {value} }}"),
                "",
            ))
            .expect("parse should succeed");
            let err = cfg.validate().unwrap_err().to_string();
            assert!(
                err.contains("growth_factor must be a finite number greater than 1.0"),
                "got: {err}"
            );
        }
    }

    #[test]
    fn a_backoff_factor_of_one_or_less_is_a_configuration_error() {
        let cfg = parse(&flow_kdl("flow_control { backoff_factor 1.0 }", ""))
            .expect("parse should succeed");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("backoff_factor must be a finite number greater than 1.0"),
            "got: {err}"
        );
    }

    #[test]
    fn a_non_finite_growth_or_backoff_factor_is_a_configuration_error() {
        for key in ["growth_factor", "backoff_factor"] {
            for value in [r#""inf""#, r#""nan""#] {
                let cfg = parse(&flow_kdl(&format!("flow_control {{ {key} {value} }}"), ""))
                    .unwrap_or_else(|e| panic!("parse should succeed for {key} {value}: {e}"));
                let err = cfg.validate().unwrap_err().to_string();
                assert!(
                    err.contains(&format!("{key} must be a finite number greater than 1.0")),
                    "got: {err}"
                );
            }
        }
    }

    #[test]
    fn an_improve_threshold_outside_the_unit_range_is_a_configuration_error() {
        for value in ["1.0", "-0.1", r#""inf""#, r#""nan""#] {
            let cfg = parse(&flow_kdl(
                &format!("flow_control {{ improve_threshold {value} }}"),
                "",
            ))
            .expect("parse should succeed");
            let err = cfg.validate().unwrap_err().to_string();
            assert!(
                err.contains("improve_threshold must be within 0.0..1.0"),
                "got: {err}"
            );
        }
    }

    #[test]
    fn zero_samples_per_arm_is_a_configuration_error() {
        let cfg = parse(&flow_kdl("flow_control { min_samples_per_arm 0 }", ""))
            .expect("parse should succeed");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("min_samples_per_arm must be at least 1"),
            "got: {err}"
        );
    }

    #[test]
    fn a_source_flow_control_error_names_the_workflow_and_source() {
        let cfg = parse(&flow_kdl("", "flow_control { growth_factor 0.5 }"))
            .expect("parse should succeed");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains(
                "workflow 'w' source 'in': flow_control: growth_factor must be a finite \
                 number greater than 1.0"
            ),
            "got: {err}"
        );
    }

    #[test]
    fn an_unknown_flow_control_key_is_rejected() {
        let err = parse(&flow_kdl("flow_control { batch_size 10 }", ""))
            .expect_err("an unknown key must be rejected");
        assert!(err.contains("batch_size"), "got: {err}");
    }

    /// A non-numeric value for a numeric flow-control key, or a non-boolean
    /// value for `enabled`, is a load-time configuration error rather than a
    /// silent fallback to `None`.
    #[test]
    fn a_non_numeric_flow_control_string_is_a_configuration_error() {
        parse(&flow_kdl(r#"flow_control { min_rows "abc" }"#, ""))
            .expect_err("a non-numeric string must not silently become the default");

        parse(&flow_kdl(r#"flow_control { enabled "maybe" }"#, ""))
            .expect_err("a non-boolean string must not silently become the default");
    }

    #[test]
    fn window_config_pairs_cover_the_geometry() {
        let window = WindowConfig {
            spec: saci_core::window_spec::WindowSpec::Sliding {
                size_ms: 60_000,
                slide_ms: 10_000,
                offset_ms: 500,
            },
            time_field: "timestamp_ms".to_string(),
            key_fields: vec!["category".to_string(), "region".to_string()],
            allowed_lateness_ms: 5_000,
        };
        let pairs = window.config_pairs();
        let get = |key: &str| {
            pairs
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.clone())
                .unwrap_or_else(|| panic!("missing {key}"))
        };
        assert_eq!(get("window.kind"), "sliding");
        assert_eq!(get("window.size_ms"), "60000");
        assert_eq!(get("window.slide_ms"), "10000");
        assert_eq!(get("window.offset_ms"), "500");
        assert_eq!(get("window.time_field"), "timestamp_ms");
        assert_eq!(get("window.key_fields"), "category,region");
        assert_eq!(get("window.allowed_lateness_ms"), "5000");

        // The injected keys must not clobber a key the operator already wrote.
        let mut spec = std::collections::HashMap::new();
        spec.insert("window.kind".to_string(), "custom".to_string());
        for (key, value) in pairs {
            spec.entry(key).or_insert(value);
        }
        assert_eq!(spec.get("window.kind").map(String::as_str), Some("custom"));
    }

    #[test]
    fn window_block_is_rejected_in_cluster_mode() {
        let raw = r#"
mode "cluster"
bootstrap #true

node id=1 data_dir="/tmp/saci"

peer id=1 addr="127.0.0.1:9000"

workflow "w" {
    wasm "p" module="p.wasm" {
        window kind="tumbling" size_ms=1000 time_field="ts"
    }
}
"#;
        let cfg = parse(raw).expect("parse");
        let err = cfg
            .validate()
            .expect_err("cluster mode cannot honour a window block");
        assert!(err.to_string().contains("window"), "got: {err}");
    }

    // ── variables block ──────────────────────────────────────────────────────

    const VARIABLES_WORKFLOW: &str = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci-test"

variables {
    mod_dir "/tmp/saci-modules"
}

workflow "w" {
    wasm "p" module="${mod_dir}/p.wasm"
}
"#;

    #[test]
    fn declared_variables_substitute_into_wasm_module_paths() {
        let mut file = NamedTempFile::new().expect("tempfile");
        file.write_all(VARIABLES_WORKFLOW.as_bytes())
            .expect("write");

        let cfg = ServiceConfig::load(file.path()).expect("load");
        assert_eq!(
            cfg.variables.get("mod_dir").map(String::as_str),
            Some("/tmp/saci-modules")
        );
        #[cfg(feature = "wasm")]
        assert_eq!(
            cfg.workflows[0].wasm[0].module.as_deref(),
            Some("/tmp/saci-modules/p.wasm")
        );
    }

    #[test]
    fn a_declared_variable_shadows_a_same_named_env_var_on_load() {
        unsafe { std::env::set_var("SACI_TEST_LOAD_SHADOW", "/from-env") };
        let raw = format!(
            r#"
mode "standalone"

node id=1 data_dir="/tmp/saci-test"

variables {{
    {name} "/from-file"
}}

workflow "w" {{
    wasm "p" module="${{{name}}}/p.wasm"
}}
"#,
            name = "SACI_TEST_LOAD_SHADOW"
        );
        let mut file = NamedTempFile::new().expect("tempfile");
        file.write_all(raw.as_bytes()).expect("write");

        let cfg = ServiceConfig::load(file.path()).expect("load");
        assert_eq!(
            cfg.variables
                .get("SACI_TEST_LOAD_SHADOW")
                .map(String::as_str),
            Some("/from-file")
        );
        #[cfg(feature = "wasm")]
        assert_eq!(
            cfg.workflows[0].wasm[0].module.as_deref(),
            Some("/from-file/p.wasm")
        );
        unsafe { std::env::remove_var("SACI_TEST_LOAD_SHADOW") };
    }

    #[test]
    fn an_undeclared_variable_is_a_load_error() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci-test"

workflow "w" {
    wasm "p" module="${SACI_TEST_UNDECLARED_NOPE}/p.wasm"
}
"#;
        let mut file = NamedTempFile::new().expect("tempfile");
        file.write_all(raw.as_bytes()).expect("write");

        let err = ServiceConfig::load(file.path()).expect_err("undeclared variable");
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message()
                .contains("'${SACI_TEST_UNDECLARED_NOPE}' is not set and has no default"),
            "got: {err}"
        );
    }

    // ── heal block ───────────────────────────────────────────────────────────

    /// The trivial workflow with `global` at the top level and `local` inside
    /// the sink node.
    fn heal_kdl(global: &str, local: &str) -> String {
        format!(
            r#"
mode "standalone"

node id=1 data_dir="/tmp/saci-test"

{global}

workflow "w" {{
    source "in" type="NoopSource" component="X"
    sink "out" type="NoopSink" component="X" {{
        {local}
    }}
    link from="in" to="out"
}}
"#
        )
    }

    /// The settings a config resolves for sink `out`.
    fn resolved_heal(cfg: &ServiceConfig) -> HealSettings {
        cfg.workflows[0].sinks[0]
            .heal
            .clone()
            .unwrap_or_default()
            .resolve(&cfg.heal, HealSettings::default())
    }

    #[test]
    fn heal_is_on_by_default_when_no_block_is_declared() {
        let cfg = parse(&minimal_standalone_kdl()).expect("parse should succeed");
        cfg.validate().expect("the default policy is valid");
        assert_eq!(cfg.heal, HealConfig::default());

        let defaults = HealSettings::default();
        assert!(defaults.enabled, "a connector heals with no config at all");
        assert_eq!(defaults.after_failures, 3);
        assert_eq!(defaults.base_delay_ms, 1_000);
        assert_eq!(defaults.max_delay_ms, 60_000);
        assert_eq!(defaults.max_attempts, 0, "0 never gives up");
    }

    #[test]
    fn a_node_heal_block_layers_over_the_top_level_one() {
        let cfg = parse(&heal_kdl(
            "heal { after_failures 5; base_delay_ms 250; max_attempts 9 }",
            "heal { after_failures 1 }",
        ))
        .expect("parse should succeed");
        cfg.validate().expect("valid");

        let settings = resolved_heal(&cfg);
        assert_eq!(settings.after_failures, 1, "the node's own key wins");
        assert_eq!(settings.base_delay_ms, 250, "the top level fills the rest");
        assert_eq!(settings.max_attempts, 9);
        assert!(settings.enabled, "neither block turned it off");
    }

    #[test]
    fn a_top_level_heal_block_parses_every_key() {
        let cfg = parse(&heal_kdl(
            r#"heal {
    enabled #true
    after_failures 2
    base_delay_ms 500
    multiplier 1.5
    max_delay_ms 20000
    jitter 0.25
    max_attempts 7
}"#,
            "",
        ))
        .expect("parse should succeed");
        cfg.validate().expect("valid");

        let settings = resolved_heal(&cfg);
        assert!(settings.enabled);
        assert_eq!(settings.after_failures, 2);
        assert_eq!(settings.base_delay_ms, 500);
        assert!((settings.multiplier - 1.5).abs() < f64::EPSILON);
        assert_eq!(settings.max_delay_ms, 20_000);
        assert!((settings.jitter - 0.25).abs() < f64::EPSILON);
        assert_eq!(settings.max_attempts, 7);
    }

    #[test]
    fn string_valued_heal_keys_parse_so_env_substitution_works() {
        let cfg = parse(&heal_kdl(
            r#"heal { enabled "false"; after_failures "6"; multiplier "1.25" }"#,
            "",
        ))
        .expect("parse should succeed");
        cfg.validate().expect("valid");

        let settings = resolved_heal(&cfg);
        assert!(!settings.enabled);
        assert_eq!(settings.after_failures, 6);
        assert!((settings.multiplier - 1.25).abs() < f64::EPSILON);
    }

    #[test]
    fn an_out_of_range_heal_key_is_rejected_rather_than_clamped() {
        for (block, fragment) in [
            (
                "heal { after_failures 0 }",
                "after_failures must be at least 1",
            ),
            (
                "heal { base_delay_ms 0 }",
                "base_delay_ms must be at least 1",
            ),
            ("heal { multiplier 0.5 }", "multiplier must be at least 1.0"),
            ("heal { jitter 2.0 }", "jitter must be within 0.0..=1.0"),
            (
                "heal { base_delay_ms 100; max_delay_ms 10 }",
                "max_delay_ms (10) must be at least base_delay_ms (100)",
            ),
        ] {
            let cfg = parse(&heal_kdl(block, "")).expect("parse should succeed");
            let err = cfg.validate().unwrap_err().to_string();
            assert!(
                err.contains(fragment) && err.contains("heal:"),
                "{block} must be refused naming its key, got: {err}"
            );
        }
    }

    #[test]
    fn a_node_heal_error_names_the_workflow_and_the_node() {
        let cfg = parse(&heal_kdl("", "heal { jitter 3.0 }")).expect("parse should succeed");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("workflow 'w' sink 'out': heal"),
            "the error must name where the key is, got: {err}"
        );
    }

    #[test]
    fn an_unknown_heal_key_is_rejected() {
        let err = parse(&heal_kdl("heal { retries 3 }", ""))
            .expect_err("an unknown key must be rejected");
        assert!(err.contains("retries"), "got: {err}");
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn a_cluster_config_with_a_heal_block_is_rejected() {
        let raw = r#"
mode "cluster"
bootstrap #true

node id=1 data_dir="/tmp/saci-test"

peer id=1 addr="127.0.0.1:9000"

heal { after_failures 2 }

workflow "w" {
    wasm "p" module="p.wasm"
}
"#;
        let cfg = parse(raw).expect("parse should succeed");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("mode \"cluster\" does not take a `heal` block"),
            "got: {err}"
        );
    }

    /// The three shapes KDL writes a `dlq` block in.
    fn dlq_kdl(block: &str) -> String {
        format!(
            r#"
mode "standalone"

node id=1 data_dir="/tmp/saci-test"

workflow "orders" {{
    source "in" type="NoopSource" component="X"
    sink "out" type="NoopSink" component="X"
    link from="in" to="out"
    {block}
}}
"#
        )
    }

    #[test]
    fn every_dlq_shape_parses_to_the_same_defaults() {
        for block in ["dlq", "dlq \"redb\""] {
            let cfg = parse(&dlq_kdl(block)).unwrap_or_else(|e| panic!("{block}: {e}"));
            let dlq = cfg.workflows[0].dlq.as_ref().expect("a declared block");
            assert_eq!(dlq.0.store, "redb", "{block}");
            assert_eq!(dlq.0.replay, DlqReplayPoint::BeforeSources, "{block}");
            assert!(dlq.0.config.is_empty(), "{block}");
            cfg.validate().unwrap_or_else(|e| panic!("{block}: {e}"));
        }
    }

    #[test]
    fn a_dlq_table_carries_the_store_the_replay_point_and_both_halves() {
        let cfg = parse(&dlq_kdl(
            r#"dlq "kafka" {
        replay "after_sources"
        brokers "broker:9092"
        topic "saci-dlq-orders"
        source { group_id "saci-dlq-orders" }
        sink { acks "all" }
    }"#,
        ))
        .expect("the table form parses");
        cfg.validate().expect("a kafka store is valid");
        let dlq = &cfg.workflows[0].dlq.as_ref().expect("a declared block").0;

        assert_eq!(dlq.store, "kafka");
        assert_eq!(dlq.replay, DlqReplayPoint::AfterSources);
        assert_eq!(dlq.config["brokers"], ConfigValue::from("broker:9092"));
        assert_eq!(dlq.config["topic"], ConfigValue::from("saci-dlq-orders"));
        assert_eq!(dlq.source["group_id"], ConfigValue::from("saci-dlq-orders"));
        assert_eq!(dlq.sink["acks"], ConfigValue::from("all"));
        assert!(
            !dlq.config.contains_key("source") && !dlq.config.contains_key("sink"),
            "the half blocks are their own fields, not shared keys"
        );
    }

    #[test]
    fn an_unknown_dlq_store_is_refused_by_name() {
        let cfg = parse(&dlq_kdl("dlq \"mongodb\"")).expect("any store name parses");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("dlq store 'mongodb' is not one of redb, kafka, nats"),
            "got: {err}"
        );
    }

    #[test]
    fn an_unknown_replay_point_is_refused_by_serde() {
        let err = parse(&dlq_kdl("dlq \"redb\" { replay \"sometimes\" }"))
            .expect_err("only the two declared points parse");
        assert!(err.contains("sometimes"), "got: {err}");
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn a_cluster_config_with_a_dlq_block_is_rejected() {
        let raw = r#"
mode "cluster"
bootstrap #true

node id=1 data_dir="/tmp/saci-test"

peer id=1 addr="127.0.0.1:9000"

workflow "w" {
    wasm "p" module="p.wasm"
    dlq "redb"
}
"#;
        let cfg = parse(raw).expect("parse should succeed");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("mode \"cluster\" does not take a `dlq` block"),
            "got: {err}"
        );
    }

    #[test]
    fn a_bad_typed_value_in_the_table_form_is_refused_with_a_specific_error() {
        // `DlqConfig::deserialize` goes through the value tree rather than
        // an untagged enum precisely so this stays a specific type-mismatch
        // error naming the offending value, not the generic "data did not
        // match any variant" an untagged enum's failed-branch collapse would
        // give.
        let err = parse(&dlq_kdl("dlq \"redb\" { replay 42 }"))
            .expect_err("a number is not a legal replay point");
        assert!(
            err.contains("42") && err.contains("invalid type"),
            "the bad value is still diagnosable: {err}"
        );
        assert!(
            !err.contains("did not match any variant"),
            "the value tree route must not collapse into the untagged-enum message: {err}"
        );
    }

    #[test]
    fn unknown_keys_land_in_the_flattened_config_not_in_either_half() {
        let cfg = parse(&dlq_kdl("dlq \"redb\" { directory \"/var/lib/saci/dlq\" }"))
            .expect("an unrecognised top-level key is not a parse error");
        let dlq = &cfg.workflows[0].dlq.as_ref().expect("a declared block").0;

        assert_eq!(
            dlq.config["directory"],
            ConfigValue::from("/var/lib/saci/dlq"),
            "an unknown top-level key lands in the flatten"
        );
        assert!(
            !dlq.config.contains_key("source") && !dlq.config.contains_key("sink"),
            "the half blocks never leak into the shared flatten"
        );
        assert_eq!(
            dlq.source,
            default_config(),
            "no source block was declared, so the half stays at its empty default"
        );
        assert_eq!(
            dlq.sink,
            default_config(),
            "no sink block was declared, so the half stays at its empty default"
        );
    }

    #[test]
    fn a_workflow_with_two_dlq_blocks_is_refused() {
        // `dlq` carries plain `#[serde(default)]`, not `one_or_many`: a
        // second declaration is not silently merged or last-wins, it turns
        // the KDL value into a sequence `DlqConfig::deserialize` never
        // matches (its `ConfigValue::String` arm aside, everything else,
        // sequence included, falls through to `DlqBlock::deserialize`, which
        // expects a map).
        let raw = dlq_kdl("dlq \"redb\"\n    dlq \"kafka\"");
        let err = parse(&raw).expect_err("a repeated dlq block is refused, not merged");
        assert!(
            err.contains("invalid type") && err.contains("sequence"),
            "got: {err}"
        );
    }
}
