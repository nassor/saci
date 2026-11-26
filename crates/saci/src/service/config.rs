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
//! run "interval" every_ms=5000
//!
//! workflow "etl" name="ETL" {
//!     wasm "transform" path="pipelines/transform.wasm" {
//!         config batch_size="1000"
//!     }
//! }
//! ```
//!
//! One `workflow` declares the whole DAG: sources, `wasm`/`plugin` processors,
//! sinks and codecs, each with a mandatory id and an optional name,
//! connected by explicit `link from="..." to="..."` declarations. See
//! [`WorkflowSpec`].
//!
//! Cluster mode instead sets `mode "cluster"` and a `cluster { }` block: one
//! `peer` node (`id`, `addr`) per cluster member, `bootstrap` on the first
//! node, and the timing keys. It takes no `run` node and no `store` block:
//! cluster state is the raft-replicated `cluster-app.redb` under
//! `node.data_dir`. A cluster-mode workflow declares exactly one
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
//!
//! ## Writing a config
//!
//! A config built in code is written with [`ServiceConfig::to_kdl`]:
//! [`ServiceConfig::standalone`] and the `new` constructor of each spec
//! ([`WorkflowSpec::new`], [`SourceSpec::new`], [`SinkSpec::new`],
//! [`WasmSpec::new`], ...) fill in every field but the required ones, and the
//! public fields take the rest. The text holds only what differs from the
//! defaults and is the same on every call, so a generated file can be
//! committed and diffed. See [`crate::config::to_kdl_string`] for the layout
//! and what a round trip through [`ServiceConfig::from_kdl`] preserves.
//!
//! A config in the v0.2.0 spelling (`type=` on a source, `run_mode`,
//! `transformer`, `schema_fields`, ...) is refused, and the error names what
//! each old key became.
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::config::{ConfigMap, ConfigValue, one_or_many};
use crate::error::{SaciError, SaciResult};

pub(super) mod encryption;
pub(super) mod flexible;
mod http;
mod legacy;
mod mode;
mod node;
mod observability;
mod processor;
mod retry;
mod store;
#[cfg(test)]
pub(super) mod test_support;
mod window;
mod workflow;

pub use self::encryption::{
    CodecEncryptionConfig, KeySpec, NodeCryptoConfig, ServiceEncryptionConfig,
};
pub use self::http::HttpConfig;
pub use self::mode::{ClusterConfig, PeerSpec, RunMode, ServiceMode, StandaloneConfig};
pub use self::node::NodeConfig;
pub use self::observability::{LogFormat, ObservabilityConfig};
pub use self::processor::{PluginSpec, WasmSpec};
pub use self::retry::RetryConfig;
pub use self::store::StoreConfig;
pub use self::window::WindowConfig;
pub(crate) use self::workflow::NodeKind;
pub use self::workflow::{CodecSpec, LinkSpec, SchemaSpec, SinkSpec, SourceSpec, WorkflowSpec};
pub use super::dlq::{DlqBlock, DlqConfig, DlqReplayPoint};
pub use super::flow::FlowControlConfig;
pub use super::heal::HealConfig;

/// Default opaque per-instance config: an empty table.
///
/// Serde's own `Default` for the value type is null, which every factory
/// rejects.
pub(super) fn default_config() -> ConfigValue {
    ConfigValue::Object(ConfigMap::new())
}

/// Whether an opaque per-instance `config` is the empty table that omitting it
/// would give, so serializing it adds nothing.
pub(super) fn is_default_config(value: &ConfigValue) -> bool {
    matches!(value, ConfigValue::Object(table) if table.is_empty())
}

/// Whether `value` equals its type's default, for `skip_serializing_if`.
pub(super) fn is_default<T: Default + PartialEq>(value: &T) -> bool {
    *value == T::default()
}

/// Whether the `run` node would be the one an omitted node means.
fn run_is_default(run: &Option<RunMode>) -> bool {
    matches!(run, None | Some(RunMode::Continuous))
}

/// Serialize a string map with its entries sorted by key.
///
/// A `HashMap` iterates in a different order in every process, so a config
/// written from one would be different text on every run. The field keeps its
/// type; only the order it is written in is fixed.
pub(super) fn serialize_sorted<S>(
    map: &HashMap<String, String>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::SerializeMap as _;

    let mut entries: Vec<(&String, &String)> = map.iter().collect();
    entries.sort_unstable_by(|left, right| left.0.cmp(right.0));
    let mut out = serializer.serialize_map(Some(entries.len()))?;
    for (key, value) in entries {
        out.serialize_entry(key, value)?;
    }
    out.end()
}

/// Top-level service configuration.
///
/// Load from a KDL file with [`ServiceConfig::load`]:
///
/// ```no_run
/// # #[cfg(feature = "service")]
/// # {
/// use saci::service::ServiceConfig;
/// let cfg = ServiceConfig::load("saci.kdl").unwrap();
/// # }
/// ```
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(try_from = "RawServiceConfig", into = "RawServiceConfig")]
pub struct ServiceConfig {
    /// Node identity and storage.
    pub node: NodeConfig,
    /// Runtime mode: the `mode` node with the `run` node (standalone) or the
    /// `cluster` block (cluster) that belongs to it.
    pub mode: ServiceMode,
    /// Persistent store declaration; `None` keeps the in-process/redb
    /// backends as today.
    pub store: Option<StoreConfig>,
    /// The workflows this process runs. One or more `workflow` blocks; a
    /// single block deserializes as a one-element list.
    pub workflows: Vec<WorkflowSpec>,
    /// HTTP control-plane options.
    pub http: HttpConfig,
    /// Logging / observability options.
    pub observability: ObservabilityConfig,
    /// Adaptive flow control for every source. An omitted block is the
    /// enabled defaults.
    pub flow_control: FlowControlConfig,
    /// Connector self-healing for every source and sink. An omitted block is
    /// the enabled defaults.
    pub heal: HealConfig,
    /// Optional keyring and default AEAD algorithm for pipeline encryption.
    pub encryption: Option<ServiceEncryptionConfig>,
    /// Names usable as `${name}` placeholders anywhere in this file. A
    /// declared name wins over a same-named process env var, so the file is
    /// self-contained.
    pub variables: HashMap<String, String>,
}

/// The document's top-level keys exactly as KDL spells them.
///
/// [`ServiceConfig`] reads and writes through this mirror, so a misspelled
/// top-level key is an error (`deny_unknown_fields`) and `mode`, `run` and
/// `cluster` fold into the single [`ServiceMode`] enum.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawServiceConfig {
    mode: String,
    node: NodeConfig,
    #[serde(default, skip_serializing_if = "run_is_default")]
    run: Option<RunMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cluster: Option<ClusterConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    store: Option<StoreConfig>,
    #[serde(
        default,
        skip_serializing_if = "HashMap::is_empty",
        serialize_with = "serialize_sorted"
    )]
    variables: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "is_default")]
    http: HttpConfig,
    #[serde(default, skip_serializing_if = "is_default")]
    observability: ObservabilityConfig,
    #[serde(default, skip_serializing_if = "is_default")]
    flow_control: FlowControlConfig,
    #[serde(default, skip_serializing_if = "is_default")]
    heal: HealConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    encryption: Option<ServiceEncryptionConfig>,
    #[serde(rename = "workflow", deserialize_with = "one_or_many")]
    workflows: Vec<WorkflowSpec>,
}

impl TryFrom<RawServiceConfig> for ServiceConfig {
    type Error = String;

    fn try_from(raw: RawServiceConfig) -> Result<Self, Self::Error> {
        let mode = match raw.mode.as_str() {
            "standalone" => {
                if raw.cluster.is_some() {
                    return Err(r#"mode "standalone" takes no cluster block"#.to_string());
                }
                ServiceMode::Standalone {
                    config: StandaloneConfig {
                        run: raw.run.unwrap_or_default(),
                    },
                }
            }
            "cluster" => {
                if raw.run.is_some() {
                    return Err(r#"mode "cluster" takes no run node"#.to_string());
                }
                let Some(config) = raw.cluster else {
                    return Err(r#"mode "cluster" requires a cluster block"#.to_string());
                };
                ServiceMode::Cluster { config }
            }
            _ => return Err(r#"mode must be "standalone" or "cluster""#.to_string()),
        };
        Ok(Self {
            node: raw.node,
            mode,
            store: raw.store,
            workflows: raw.workflows,
            http: raw.http,
            observability: raw.observability,
            flow_control: raw.flow_control,
            heal: raw.heal,
            encryption: raw.encryption,
            variables: raw.variables,
        })
    }
}

impl From<ServiceConfig> for RawServiceConfig {
    fn from(config: ServiceConfig) -> Self {
        let (mode, run, cluster) = match config.mode {
            ServiceMode::Standalone { config: standalone } => {
                ("standalone", Some(standalone.run), None)
            }
            ServiceMode::Cluster { config: cluster } => ("cluster", None, Some(cluster)),
        };
        Self {
            node: config.node,
            mode: mode.to_string(),
            run,
            cluster,
            store: config.store,
            workflows: config.workflows,
            http: config.http,
            observability: config.observability,
            flow_control: config.flow_control,
            heal: config.heal,
            encryption: config.encryption,
            variables: config.variables,
        }
    }
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
    /// A standalone config: `node`, the `run` mode, and nothing else yet.
    ///
    /// Every other field is its default: no store, the default `http`,
    /// `observability`, `flow_control` and `heal` blocks, no `variables` and no
    /// workflows. Push the workflows onto [`workflows`](Self::workflows) and
    /// set any other public field directly.
    pub fn standalone(node: NodeConfig, run: RunMode) -> Self {
        Self {
            node,
            mode: ServiceMode::Standalone {
                config: StandaloneConfig { run },
            },
            store: None,
            workflows: Vec::new(),
            http: HttpConfig::default(),
            observability: ObservabilityConfig::default(),
            flow_control: FlowControlConfig::default(),
            heal: HealConfig::default(),
            encryption: None,
            variables: HashMap::new(),
        }
    }

    /// Load a [`ServiceConfig`] from a KDL file at `path`.
    ///
    /// The file is read and its text handed to [`from_kdl`](Self::from_kdl).
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
        Self::from_kdl(&raw)
    }

    /// Parse a [`ServiceConfig`] from KDL text.
    ///
    /// The text's declared `variables` and env-var placeholders are
    /// substituted (declared names win over same-named env vars), the KDL is
    /// parsed, and semantic validation is applied before returning.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] for parse errors or any validation
    /// constraint violation. A config written in the v0.2.0 spelling (`type=`
    /// on a source, `run_mode`, `transformer`, `schema_fields`, ...) is still
    /// refused, but the message ends with one `hint:` line per old spelling
    /// naming what replaced it.
    pub fn from_kdl(text: &str) -> SaciResult<Self> {
        let value_pre = crate::config::from_kdl_str(text)?;
        let declared = extract_declared_variables(&value_pre)?;
        let value = crate::config::from_kdl_str_with_vars(text, &declared)?;
        let config = ServiceConfig::deserialize(value).map_err(|e| {
            let mut message = format!("parsing config: {e}");
            for hint in legacy::hints(&value_pre) {
                message.push_str("\n  hint: ");
                message.push_str(&hint);
            }
            SaciError::configuration(message)
        })?;
        config.validate()?;
        Ok(config)
    }

    /// Write this config as KDL text that [`from_kdl`](Self::from_kdl) reads
    /// back into an equal config.
    ///
    /// The config is serialized through the document's own top-level keys
    /// (`mode`, `run`, `cluster`, `workflow`, ...) and written with
    /// [`to_kdl`](crate::config::to_kdl), which documents the layout. A field
    /// at its default is left out, so the text holds what was declared and
    /// nothing else: no `retry` block on a node that retries by default, no
    /// `http` block when the control plane is untouched, no `run` node for
    /// the default continuous mode.
    ///
    /// The text is the same on every call: entries come out in declaration
    /// order and the maps (`variables`, a processor's `config`) are sorted by
    /// key.
    ///
    /// A string is written verbatim, so a `${NAME}` placeholder in it is
    /// expanded by the next [`from_kdl`](Self::from_kdl), and a value that
    /// really holds `${` cannot be spelled. A config with no workflow writes
    /// no `workflow` node, which `from_kdl` refuses, as it does a schema
    /// whose `field` list is empty.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when a value has no KDL spelling,
    /// naming its key path: see [`to_kdl_string`](crate::config::to_kdl_string).
    pub fn to_kdl(&self) -> SaciResult<String> {
        crate::config::to_kdl(self)
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
        if let Some(encryption) = &self.encryption {
            encryption.validate()?;
        }

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

        // Channel names pair exactly one channel sink with one channel source.
        // A dangling half hangs: a source whose registry-held sender never
        // drops, or a sink whose registry-held receiver is never drained.
        let mut channels: HashMap<&str, (Option<&str>, Option<&str>)> = HashMap::new();
        for wf in &self.workflows {
            for s in &wf.sources {
                if s.connector == "channel"
                    && let Some(name) = s.config.get("name").and_then(ConfigValue::as_str)
                {
                    let entry = channels.entry(name).or_default();
                    if entry.0.is_some() {
                        return Err(SaciError::configuration(format!(
                            "channel '{name}': more than one channel source declared"
                        )));
                    }
                    entry.0 = Some(s.id.as_str());
                }
            }
            for s in &wf.sinks {
                if s.connector == "channel"
                    && let Some(name) = s.config.get("name").and_then(ConfigValue::as_str)
                {
                    let entry = channels.entry(name).or_default();
                    if entry.1.is_some() {
                        return Err(SaciError::configuration(format!(
                            "channel '{name}': more than one channel sink declared"
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
                        "channel '{name}': declares a channel source but no channel sink"
                    )));
                }
                (None, Some(_)) => {
                    return Err(SaciError::configuration(format!(
                        "channel '{name}': declares a channel sink but no channel source"
                    )));
                }
                (Some(_), Some(_)) => {}
                (None, None) => unreachable!("an entry exists only when a half was seen"),
            }
        }

        // The HTTP bind address is validated before the store, so a config
        // with both problems reports the listener first.
        if self.http.enabled {
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

impl FromStr for ServiceConfig {
    type Err = SaciError;

    /// Parse KDL text with [`ServiceConfig::from_kdl`].
    fn from_str(text: &str) -> SaciResult<Self> {
        Self::from_kdl(text)
    }
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::test_support::*;
    use super::*;
    use std::io::Write;
    use std::path::PathBuf;
    use tempfile::NamedTempFile;

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn full_config() -> ServiceConfig {
        ServiceConfig {
            heal: Default::default(),
            flow_control: Default::default(),
            encryption: None,
            node: NodeConfig {
                id: 1,
                name: Some("node-1".to_string()),
                data_dir: PathBuf::from("/tmp/saci"),
            },
            mode: ServiceMode::Standalone {
                config: StandaloneConfig {
                    run: RunMode::Interval { every_ms: 5_000 },
                },
            },
            workflows: vec![WorkflowSpec {
                id: "payments".to_string(),
                name: None,
                codecs: Vec::new(),
                schemas: Vec::new(),
                sources: vec![SourceSpec {
                    heal: None,
                    flow_control: None,
                    id: "kafka_in".to_string(),
                    name: None,
                    connector: "mongodb".to_string(),
                    codec: None,
                    component: "orders".to_string(),
                    retry: RetryConfig::default(),
                    decrypt: None,
                    config: default_config(),
                }],
                wasm: Vec::new(),
                plugin: Vec::new(),
                sinks: vec![SinkSpec {
                    heal: None,
                    id: "pg_out".to_string(),
                    name: None,
                    connector: "postgresql".to_string(),
                    codec: None,
                    component: "orders".to_string(),
                    retry: RetryConfig::default(),
                    encrypt: None,
                    config: default_config(),
                }],
                links: Vec::new(),
                dlq: None,
            }],
            store: None,
            http: HttpConfig {
                bind: "0.0.0.0:8080".to_string(),
                enabled: true,
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
    // `full_config()` builds by hand. This config is never `.validate()`d:
    // the source and sink are deliberately unlinked, to pin down the parse
    // shape without also asserting the graph rules.
    #[test]
    fn test_full_standalone_config_deserialises() {
        let raw = r#"
mode "standalone"

node id=1 name="node-1" data_dir="/tmp/saci"

run "interval" every_ms=5000

workflow "payments" {
    source "kafka_in" connector="mongo" component="orders"

    sink "pg_out" connector="postgresql" component="orders"
}

http bind="0.0.0.0:8080" enabled=#true

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
        assert!(restored.http.enabled);
        assert_eq!(restored.observability.log_level, "debug");
        assert_eq!(restored.observability.log_format, LogFormat::Json);
        match restored.mode {
            ServiceMode::Standalone { config } => {
                assert_eq!(config.run, RunMode::Interval { every_ms: 5_000 });
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
        assert!(cfg.http.enabled);
        assert_eq!(cfg.observability.log_level, "error");
        assert_eq!(cfg.observability.log_format, LogFormat::Pretty);
        cfg.validate().expect("trivial source-to-sink workflow");
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

cluster {{
    peer id=1 addr="127.0.0.1:9000"
    peer id=2 addr="127.0.0.2:9000"
}}
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
    #[cfg(feature = "wasm-host")]
    #[test]
    fn test_cluster_with_store_block_rejected() {
        let raw = r#"
mode "cluster"

node id=1 data_dir="/tmp/saci"

cluster {
    bootstrap #true
    peer id=1 addr="127.0.0.1:9000"
}

store "redb" {
    path "/tmp/saci-state.redb"
}

workflow "w" {
    wasm "p" path="p.wasm"
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
    #[cfg(feature = "wasm-host")]
    #[test]
    fn test_cluster_with_flow_control_block_rejected() {
        let raw = r#"
mode "cluster"

node id=1 data_dir="/tmp/saci"

cluster {
    bootstrap #true
    peer id=1 addr="127.0.0.1:9000"
}

flow_control {
    start_rows 8192
}

workflow "w" {
    wasm "p" path="p.wasm"
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
    #[cfg(feature = "wasm-host")]
    #[test]
    fn test_cluster_flow_control_refusal_outranks_its_value_checks() {
        let raw = r#"
mode "cluster"

node id=1 data_dir="/tmp/saci"

cluster {
    bootstrap #true
    peer id=1 addr="127.0.0.1:9000"
}

flow_control {
    growth_factor 0.5
}

workflow "w" {
    wasm "p" path="p.wasm"
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
    #[cfg(feature = "wasm-host")]
    #[test]
    fn test_cluster_without_store_validates() {
        let raw = r#"
mode "cluster"

node id=1 data_dir="/tmp/saci"

cluster {
    bootstrap #true
    peer id=1 addr="127.0.0.1:9000"
}

workflow "w" {
    wasm "p" path="p.wasm"
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

node id=1 data_dir="/tmp/saci"

cluster {{
    lease_ttl_ms 1000
    election_timeout_ms 1000
    peer id=1 addr="127.0.0.1:9000"
}}
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
    fn from_kdl_matches_load_of_a_file_holding_the_same_text() {
        let text = minimal_standalone_kdl();
        let mut file = NamedTempFile::new().expect("tempfile");
        file.write_all(text.as_bytes()).expect("write");

        let from_text = ServiceConfig::from_kdl(&text).expect("from_kdl");
        let from_file = ServiceConfig::load(file.path()).expect("load");
        let parsed: ServiceConfig = text.parse().expect("FromStr");

        let as_json = |cfg: &ServiceConfig| serde_json::to_value(cfg).expect("serialize");
        assert_eq!(as_json(&from_text), as_json(&from_file));
        assert_eq!(as_json(&parsed), as_json(&from_file));
    }

    #[test]
    fn test_load_rejects_a_malformed_document_naming_the_position() {
        let err = ServiceConfig::from_kdl("mode \"standalone\nnode id=1\n")
            .expect_err("unterminated string");
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().starts_with("parsing KDL: 1:"),
            "error should name the position: {err}"
        );
    }

    #[test]
    fn test_a_misspelled_top_level_key_is_an_error_naming_it() {
        let doc = format!("{}\nhtpp bind=\"127.0.0.1:1\"\n", minimal_standalone_kdl());
        let err = parse(&doc).expect_err("an unknown top-level node");
        assert!(err.contains("htpp"), "error should name the key: {err}");
    }

    #[test]
    fn test_standalone_mode_with_a_cluster_block_is_an_error() {
        let doc = format!(
            "{}\ncluster {{\n    peer id=1 addr=\"127.0.0.1:9000\"\n}}\n",
            minimal_standalone_kdl()
        );
        let err = parse(&doc).expect_err("standalone takes no cluster block");
        assert!(
            err.contains(r#"mode "standalone" takes no cluster block"#),
            "{err}"
        );
    }

    #[test]
    fn test_cluster_mode_takes_a_cluster_block_and_no_run_node() {
        let base = r#"
mode "cluster"
node id=1 data_dir="/tmp/saci"
"#;
        let err = parse(&format!("{base}\n{TRIVIAL_WORKFLOW}")).expect_err("no cluster block");
        assert!(err.contains("requires a cluster block"), "{err}");

        let doc = format!(
            "{base}\nrun \"once\"\ncluster {{\n    peer id=1 addr=\"127.0.0.1:9000\"\n}}\n{TRIVIAL_WORKFLOW}"
        );
        let err = parse(&doc).expect_err("cluster takes no run node");
        assert!(err.contains("takes no run node"), "{err}");
    }

    #[test]
    fn test_a_mode_other_than_standalone_or_cluster_is_an_error() {
        let doc = minimal_standalone_kdl().replace("standalone", "solo");
        let err = parse(&doc).expect_err("unknown mode");
        assert!(
            err.contains(r#"mode must be "standalone" or "cluster""#),
            "{err}"
        );
    }

    #[cfg(feature = "wasm-host")]
    #[test]
    fn test_wasm_only_workflow_is_a_valid_entry_point() {
        let cfg = ServiceConfig {
            heal: Default::default(),
            flow_control: Default::default(),
            encryption: None,
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
                codecs: Vec::new(),
                schemas: Vec::new(),
                sources: Vec::new(),
                wasm: vec![WasmSpec {
                    id: "p".to_string(),
                    name: None,
                    path: Some("pipeline.wasm".to_string()),
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
    /// `wasm-host` on there is nothing to refuse. `--all-features` skips it, so
    /// `cargo nextest run -p saci --no-default-features --features
    /// service,connector-channel --lib` is what exercises it; the wording itself is pinned in
    /// every build by
    /// `factories::tests::every_host_refusal_names_the_feature_that_supplies_it`.
    #[cfg(not(feature = "wasm-host"))]
    #[test]
    fn test_wasm_node_without_the_host_parses_and_names_the_feature() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    wasm "transform" path="transform.wasm"
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
    #[cfg(not(feature = "plugin-host"))]
    #[test]
    fn test_plugin_node_without_the_host_parses_and_names_the_feature() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    plugin "audit" path="libaudit.so"
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
    /// Needs `wasm-host`, because the missing host is reported first and would
    /// mask this one. `cargo nextest run -p saci --no-default-features
    /// --features service,wasm-host,plugin-host,connector-channel --lib` is
    /// what exercises it.
    #[cfg(all(not(feature = "windows"), feature = "wasm-host"))]
    #[test]
    fn test_window_block_without_the_engine_parses_and_names_the_feature() {
        let raw = workflow_kdl(
            r#"
wasm "aggregate" path="aggregate.wasm" {
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
    #[cfg(all(not(feature = "windows"), feature = "plugin-host"))]
    #[test]
    fn test_plugin_window_block_without_the_engine_names_the_feature() {
        let raw = workflow_kdl(
            r#"
plugin "aggregate" path="libaggregate.so" {
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
    /// `validate_build_capabilities`: reading `cfg!(feature = "wasm-host")`
    /// instead of its negation would make every default binary refuse every
    /// wasm config, and nothing else in the suite calls the gate, since it is
    /// reached only from the binary.
    #[cfg(all(feature = "wasm-host", feature = "plugin-host"))]
    #[test]
    fn test_declared_hosts_this_build_carries_are_admitted() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    wasm "transform" path="transform.wasm"
    plugin "audit" path="libaudit.so"
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
    #[cfg(all(feature = "windows", feature = "wasm-host"))]
    #[test]
    fn test_a_window_block_is_admitted_by_a_windowing_build() {
        let raw = workflow_kdl(
            r#"
wasm "aggregate" path="aggregate.wasm" {
    window kind="tumbling" size_ms=30000 time_field="timestamp_ms"
}
"#,
        );
        let cfg = parse(&raw).expect("parse");
        crate::service::validation::validate_build_capabilities(&cfg)
            .expect("a windowing build must admit a window block");
    }

    // ── Cross-workflow rules: ServiceConfig::validate ────────────────────────

    #[test]
    fn test_two_workflow_blocks_parse_into_two_workflows() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "a" {
    source "in_a" connector="noop" component="X"
    sink "out_a" connector="noop" component="X"
    link from="in_a" to="out_a"
}

workflow "b" {
    source "in_b" connector="noop" component="Y"
    sink "out_b" connector="noop" component="Y"
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
    source "shared" connector="noop" component="X"
    sink "out_a" connector="noop" component="X"
    link from="shared" to="out_a"
}

workflow "b" {
    source "shared" connector="noop" component="Y"
    sink "out_b" connector="noop" component="Y"
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
    source "in_a" connector="noop" component="X"
    sink "out_a" connector="noop" component="X"
    link from="in_a" to="out_a"
}

workflow "dup" {
    source "in_b" connector="noop" component="Y"
    sink "out_b" connector="noop" component="Y"
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

    #[cfg(feature = "wasm-host")]
    #[test]
    fn rule_cluster_mode_requires_exactly_one_workflow() {
        let raw = r#"
mode "cluster"

node id=1 data_dir="/tmp/saci"

cluster {
    bootstrap #true
    peer id=1 addr="127.0.0.1:9000"
}

workflow "a" {
    wasm "p1" path="p1.wasm"
}

workflow "b" {
    wasm "p2" path="p2.wasm"
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
source "in" connector="channel" component="X" {
    config name="orphan"
}
sink "out" connector="noop" component="X"
link from="in" to="out"
"#,
        );
        assert!(
            err.contains("channel 'orphan': declares a channel source but no channel sink"),
            "got: {err}"
        );
    }

    #[test]
    fn rule_channel_sink_with_no_paired_source_is_rejected() {
        let err = workflow_err(
            r#"
source "in" connector="noop" component="X"
sink "out" connector="channel" component="X" {
    config name="orphan"
}
link from="in" to="out"
"#,
        );
        assert!(
            err.contains("channel 'orphan': declares a channel sink but no channel source"),
            "got: {err}"
        );
    }

    #[test]
    fn rule_duplicate_channel_source_name_is_rejected() {
        let err = workflow_err(
            r#"
source "in1" connector="channel" component="X" {
    config name="dup"
}
source "in2" connector="channel" component="X" {
    config name="dup"
}
sink "out1" connector="noop" component="X"
sink "out2" connector="noop" component="X"
link from="in1" to="out1"
link from="in2" to="out2"
"#,
        );
        assert!(
            err.contains("channel 'dup': more than one channel source declared"),
            "got: {err}"
        );
    }

    #[test]
    fn rule_duplicate_channel_sink_name_is_rejected() {
        let err = workflow_err(
            r#"
source "in1" connector="noop" component="X"
source "in2" connector="noop" component="X"
sink "out1" connector="channel" component="X" {
    config name="dup"
}
sink "out2" connector="channel" component="X" {
    config name="dup"
}
link from="in1" to="out1"
link from="in2" to="out2"
"#,
        );
        assert!(
            err.contains("channel 'dup': more than one channel sink declared"),
            "got: {err}"
        );
    }

    #[test]
    fn two_workflows_bridged_by_a_named_channel_validate() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "producer" {
    source "in" connector="noop" component="X"
    sink "bridge_out" connector="channel" component="X" {
        config name="bridge"
    }
    link from="in" to="bridge_out"
}

workflow "consumer" {
    source "bridge_in" connector="channel" component="X" {
        config name="bridge"
    }
    sink "out" connector="noop" component="X"
    link from="bridge_in" to="out"
}
"#;
        let cfg = parse(raw).expect("parse should succeed");
        cfg.validate()
            .expect("a sink in one workflow paired with a source in another is valid");
    }

    // ── variables block ──────────────────────────────────────────────────────

    const VARIABLES_WORKFLOW: &str = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci-test"

variables {
    mod_dir "/tmp/saci-modules"
}

workflow "w" {
    wasm "p" path="${mod_dir}/p.wasm"
}
"#;

    #[test]
    fn declared_variables_substitute_into_wasm_paths() {
        let cfg = ServiceConfig::from_kdl(VARIABLES_WORKFLOW).expect("load");
        assert_eq!(
            cfg.variables.get("mod_dir").map(String::as_str),
            Some("/tmp/saci-modules")
        );
        #[cfg(feature = "wasm-host")]
        assert_eq!(
            cfg.workflows[0].wasm[0].path.as_deref(),
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
    wasm "p" path="${{{name}}}/p.wasm"
}}
"#,
            name = "SACI_TEST_LOAD_SHADOW"
        );
        let cfg = ServiceConfig::from_kdl(&raw).expect("load");
        assert_eq!(
            cfg.variables
                .get("SACI_TEST_LOAD_SHADOW")
                .map(String::as_str),
            Some("/from-file")
        );
        #[cfg(feature = "wasm-host")]
        assert_eq!(
            cfg.workflows[0].wasm[0].path.as_deref(),
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
    wasm "p" path="${SACI_TEST_UNDECLARED_NOPE}/p.wasm"
}
"#;
        let err = ServiceConfig::from_kdl(raw).expect_err("undeclared variable");
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message()
                .contains("'${SACI_TEST_UNDECLARED_NOPE}' is not set and has no default"),
            "got: {err}"
        );
    }

    #[cfg(feature = "wasm-host")]
    #[test]
    fn a_cluster_config_with_a_heal_block_is_rejected() {
        let raw = r#"
mode "cluster"

node id=1 data_dir="/tmp/saci-test"

cluster {
    bootstrap #true
    peer id=1 addr="127.0.0.1:9000"
}

heal { after_failures 2 }

workflow "w" {
    wasm "p" path="p.wasm"
}
"#;
        let cfg = parse(raw).expect("parse should succeed");
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("mode \"cluster\" does not take a `heal` block"),
            "got: {err}"
        );
    }

    #[cfg(feature = "wasm-host")]
    #[test]
    fn a_cluster_config_with_a_dlq_block_is_rejected() {
        let raw = r#"
mode "cluster"

node id=1 data_dir="/tmp/saci-test"

cluster {
    bootstrap #true
    peer id=1 addr="127.0.0.1:9000"
}

workflow "w" {
    wasm "p" path="p.wasm"
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

    // ── Writer ───────────────────────────────────────────────────────────────

    /// Every `.kdl` file under `examples/`, sorted: the files the repository
    /// tells an operator to copy, so the ones a writer must never break.
    fn example_files() -> Vec<PathBuf> {
        fn walk(dir: &std::path::Path, found: &mut Vec<PathBuf>) {
            for entry in std::fs::read_dir(dir).expect("readable directory") {
                let path = entry.expect("directory entry").path();
                if path.is_dir() {
                    walk(&path, found);
                } else if path.extension().is_some_and(|extension| extension == "kdl") {
                    found.push(path);
                }
            }
        }

        let mut found = Vec::new();
        walk(
            &PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples"),
            &mut found,
        );
        found.sort();
        found
    }

    fn as_json(config: &ServiceConfig) -> serde_json::Value {
        serde_json::to_value(config).expect("serialize")
    }

    /// Examples whose typed fields (`node id`, `cluster bootstrap`) hold a
    /// `${...}` placeholder, which only substitution turns into a number or a
    /// bool. The rest are read with `from_kdl_str` and no substitution, so
    /// their placeholders reach the deserializer as the strings they are.
    const TYPED_PLACEHOLDERS: &[&str] = &["cluster.kdl"];

    #[test]
    fn every_example_config_survives_a_write_and_a_read() {
        let files = example_files();
        assert!(files.len() >= 20, "examples not found: {files:?}");

        for path in files {
            let name = path.display();
            let text = std::fs::read_to_string(&path).expect("readable example");
            let file_name = path
                .file_name()
                .and_then(|n| n.to_str())
                .expect("a file name");
            let config = if TYPED_PLACEHOLDERS.contains(&file_name) {
                // Every placeholder name in the file is declared with a fixed
                // value (a declared name beats the environment), so nothing
                // here reads the process environment.
                let fixed = |name: &str| match name {
                    "SACI_NODE_ID" => "2",
                    "SACI_BOOTSTRAP" => "false",
                    _ => "fixed",
                };
                let declared: HashMap<String, String> = text
                    .split("${")
                    .skip(1)
                    .map(|rest| {
                        let end = rest.find([':', '}']).expect("closed placeholder");
                        (rest[..end].to_string(), fixed(&rest[..end]).to_string())
                    })
                    .collect();
                assert!(declared.contains_key("SACI_NODE_ID"), "{name}");
                let value = crate::config::from_kdl_str_with_vars(&text, &declared)
                    .unwrap_or_else(|e| panic!("{name}: {e}"));
                ServiceConfig::deserialize(value).unwrap_or_else(|e| panic!("{name}: {e}"))
            } else {
                parse(&text).unwrap_or_else(|e| panic!("{name}: {e}"))
            };

            let written = config
                .to_kdl()
                .unwrap_or_else(|e| panic!("{name}: writing: {e}"));
            let reread = parse(&written).unwrap_or_else(|e| {
                panic!("{name}: the written text does not parse: {e}\n{written}")
            });

            assert_eq!(as_json(&reread), as_json(&config), "{name}\n{written}");
            assert_eq!(
                reread.to_kdl().expect("writes again"),
                written,
                "{name}: writing what was read is not a fixed point"
            );
        }
    }

    /// The same files one level down, on the value tree: placeholders and every
    /// other string come back byte for byte, including the ones a typed
    /// field would reject before substitution.
    #[test]
    fn every_example_document_survives_a_write_and_a_read_as_a_value_tree() {
        for path in example_files() {
            let name = path.display();
            let text = std::fs::read_to_string(&path).expect("readable example");
            let tree = crate::config::from_kdl_str(&text).unwrap_or_else(|e| panic!("{name}: {e}"));

            let written = crate::config::to_kdl_string(&tree)
                .unwrap_or_else(|e| panic!("{name}: writing: {e}"));
            let reread = crate::config::from_kdl_str(&written).unwrap_or_else(|e| {
                panic!("{name}: the written text does not parse: {e}\n{written}")
            });

            assert_eq!(reread, tree, "{name}\n{written}");
        }
    }

    #[test]
    fn a_config_at_its_defaults_writes_only_what_it_declares() {
        let mut config =
            ServiceConfig::standalone(NodeConfig::new(1, "/tmp/saci"), RunMode::Continuous);
        let mut workflow = WorkflowSpec::new("w");
        workflow.sources.push(SourceSpec::new("in", "noop", "X"));
        workflow.sinks.push(SinkSpec::new("out", "noop", "X"));
        workflow.links.push(LinkSpec::new("in", "out"));
        config.workflows.push(workflow);

        let written = config.to_kdl().expect("writes");
        assert_eq!(
            written,
            "mode \"standalone\"\n\n\
             node id=1 data_dir=\"/tmp/saci\"\n\n\
             workflow \"w\" {\n  \
             source \"in\" connector=\"noop\" component=\"X\"\n  \
             sink \"out\" connector=\"noop\" component=\"X\"\n  \
             link from=\"in\" to=\"out\"\n}\n"
        );

        let json = as_json(&config);
        for absent in [
            "run",
            "store",
            "http",
            "observability",
            "flow_control",
            "heal",
            "variables",
        ] {
            assert!(
                json.get(absent).is_none(),
                "{absent} is at its default: {json}"
            );
        }
        let source = &json["workflow"][0]["source"][0];
        for absent in ["name", "codec", "retry", "heal", "flow_control", "config"] {
            assert!(
                source.get(absent).is_none(),
                "{absent} is at its default: {source}"
            );
        }

        let reread = ServiceConfig::from_kdl(&written).expect("reads back and validates");
        assert_eq!(as_json(&reread), json);
    }

    #[test]
    fn a_config_built_with_the_constructors_writes_every_block_it_sets() {
        use crate::window::WindowSpec;
        use serde_json::json;

        let mut config = ServiceConfig::standalone(
            NodeConfig::new(7, "/var/lib/saci"),
            RunMode::Interval { every_ms: 5_000 },
        );
        config.node.name = Some("saci-7".to_string());
        config.store = Some(StoreConfig::Redb {
            path: PathBuf::from("/var/lib/saci/state.redb"),
            batch_resume: true,
        });
        config.variables = HashMap::from([
            ("region".to_string(), "eu".to_string()),
            ("bucket".to_string(), "orders".to_string()),
        ]);
        config.http.bind = "127.0.0.1:9090".to_string();

        let mut workflow = WorkflowSpec::new("orders");
        workflow.name = Some("Orders".to_string());
        let mut codec = CodecSpec::new("csv_fmt", "csv");
        codec.config = json!({ "has_headers": true });
        workflow.codecs.push(codec);
        let mut schema = SchemaSpec::new("Order");
        schema.field = json!([
            { "id": "id", "type": "int64", "nullable": false },
            { "id": "amount", "type": "float64" },
        ]);
        workflow.schemas.push(schema);
        let mut source = SourceSpec::new("csv_orders", "file", "Order");
        source.codec = Some("csv_fmt".to_string());
        source.retry = RetryConfig {
            max_attempts: 6,
            ..RetryConfig::default()
        };
        source.config = json!({ "path": "orders.csv" });
        workflow.sources.push(source);
        let mut wasm = WasmSpec::new("process");
        wasm.path = Some("process.wasm".to_string());
        wasm.config = HashMap::from([
            ("mode".to_string(), "fast".to_string()),
            ("batch_size".to_string(), "1000".to_string()),
        ]);
        let mut window = WindowConfig::new(
            WindowSpec::Tumbling {
                size_ms: 30_000,
                offset_ms: 0,
            },
            "ts",
        );
        window.key_fields = vec!["region".to_string(), "category".to_string()];
        wasm.window = Some(window);
        workflow.wasm.push(wasm);
        let mut sink = SinkSpec::new("out", "file", "Order");
        sink.codec = Some("csv_fmt".to_string());
        sink.config = json!({ "path": "out.csv" });
        workflow.sinks.push(sink);
        workflow.links.push(LinkSpec::new("csv_orders", "process"));
        let mut routed = LinkSpec::new("process", "out");
        routed.branch = Some("ok".to_string());
        workflow.links.push(routed);
        config.workflows.push(workflow);

        let written = config.to_kdl().expect("writes");
        assert_eq!(
            written,
            r#"mode "standalone"

node id=7 name="saci-7" data_dir="/var/lib/saci"

run "interval" every_ms=5000

store "redb" path="/var/lib/saci/state.redb" batch_resume=#true

variables bucket="orders" region="eu"

http bind="127.0.0.1:9090"

workflow "orders" name="Orders" {
  codec "csv_fmt" format="csv" {
    config has_headers=#true
  }
  schema "Order" {
    field "id" type="int64" nullable=#false
    field "amount" type="float64"
  }
  source "csv_orders" connector="file" codec="csv_fmt" component="Order" {
    retry max_attempts=6
    config path="orders.csv"
  }
  wasm "process" path="process.wasm" {
    config batch_size="1000" mode="fast"
    window kind="tumbling" size_ms=30000 time_field="ts" {
      key_fields "region" "category"
    }
  }
  sink "out" connector="file" codec="csv_fmt" component="Order" {
    config path="out.csv"
  }
  link from="csv_orders" to="process"
  link from="process" to="out" branch="ok"
}
"#
        );

        let reread = parse(&written).expect("reads back");
        assert_eq!(as_json(&reread), as_json(&config));
    }

    /// A `HashMap` iterates in an order of its own per instance, so two
    /// configs built from the same entries must still write the same bytes.
    #[test]
    fn map_entries_are_written_sorted_and_identically_every_time() {
        let keys = [
            "zeta", "alpha", "mid", "beta", "omega", "gamma", "delta", "kappa", "sigma", "theta",
        ];
        let build = || {
            let pairs = || {
                keys.iter()
                    .map(|key| (key.to_string(), format!("value-of-{key}")))
            };
            let mut wasm = WasmSpec::new("wasm_node");
            wasm.config = pairs().collect();
            let mut plugin = PluginSpec::new("plugin_node");
            plugin.config = pairs().rev().collect();
            let mut workflow = WorkflowSpec::new("w");
            workflow.wasm.push(wasm);
            workflow.plugin.push(plugin);
            let mut config =
                ServiceConfig::standalone(NodeConfig::new(1, "/tmp/saci"), RunMode::Once);
            config.variables = pairs().collect();
            config.workflows.push(workflow);
            config
        };

        let first = build().to_kdl().expect("writes");
        for _ in 0..16 {
            assert_eq!(build().to_kdl().expect("writes"), first);
        }

        let mut sorted = keys;
        sorted.sort_unstable();
        for section in ["variables ", "  config "] {
            for line in first
                .lines()
                .filter(|line| line.trim_start().starts_with(section.trim_start()))
            {
                let positions: Vec<usize> = sorted
                    .iter()
                    .map(|key| {
                        line.find(&format!("{key}="))
                            .unwrap_or_else(|| panic!("{key} in {line}"))
                    })
                    .collect();
                assert!(positions.windows(2).all(|pair| pair[0] < pair[1]), "{line}");
            }
        }

        let json = as_json(&build());
        let order = |value: &serde_json::Value| -> Vec<String> {
            value
                .as_object()
                .expect("a table")
                .keys()
                .cloned()
                .collect()
        };
        let expected: Vec<String> = sorted.iter().map(|key| key.to_string()).collect();
        assert_eq!(order(&json["variables"]), expected);
        assert_eq!(order(&json["workflow"][0]["wasm"][0]["config"]), expected);
        assert_eq!(order(&json["workflow"][0]["plugin"][0]["config"]), expected);
    }

    /// A constructor is the node a file would declare with only its required
    /// keys: every other field is what deserializing leaves it as.
    #[test]
    fn constructors_build_what_a_minimal_node_deserializes_to() {
        use crate::window::WindowSpec;

        fn read<T: serde::de::DeserializeOwned>(kdl: &str, node: &str) -> T {
            let document = crate::config::from_kdl_str(kdl).expect("valid KDL");
            T::deserialize(document[node].clone()).expect("deserializes")
        }

        assert_eq!(
            format!("{:?}", NodeConfig::new(3, "/data")),
            format!(
                "{:?}",
                read::<NodeConfig>(r#"node id=3 data_dir="/data""#, "node")
            ),
        );
        assert_eq!(
            format!("{:?}", WorkflowSpec::new("w")),
            format!(
                "{:?}",
                read::<WorkflowSpec>(r#"workflow id="w""#, "workflow")
            ),
        );
        assert_eq!(
            format!("{:?}", SourceSpec::new("in", "file", "X")),
            format!(
                "{:?}",
                read::<SourceSpec>(r#"source id="in" connector="file" component="X""#, "source")
            ),
        );
        assert_eq!(
            format!("{:?}", SinkSpec::new("out", "file", "X")),
            format!(
                "{:?}",
                read::<SinkSpec>(r#"sink id="out" connector="file" component="X""#, "sink")
            ),
        );
        assert_eq!(
            format!("{:?}", CodecSpec::new("c", "csv")),
            format!(
                "{:?}",
                read::<CodecSpec>(r#"codec id="c" format="csv""#, "codec")
            ),
        );
        assert_eq!(
            format!("{:?}", LinkSpec::new("a", "b")),
            format!("{:?}", read::<LinkSpec>(r#"link from="a" to="b""#, "link")),
        );
        assert_eq!(
            format!("{:?}", WasmSpec::new("p")),
            format!("{:?}", read::<WasmSpec>(r#"wasm id="p""#, "wasm")),
        );
        assert_eq!(
            format!("{:?}", PluginSpec::new("p")),
            format!("{:?}", read::<PluginSpec>(r#"plugin id="p""#, "plugin")),
        );
        assert_eq!(
            format!(
                "{:?}",
                WindowConfig::new(WindowSpec::Session { gap_ms: 5 }, "ts")
            ),
            format!(
                "{:?}",
                read::<WindowConfig>(
                    r#"window kind="session" gap_ms=5 time_field="ts""#,
                    "window"
                )
            ),
        );

        let schema = SchemaSpec::new("Order");
        assert_eq!(schema.id, "Order");
        assert_eq!(schema.field, serde_json::json!([]));

        let standalone = ServiceConfig::standalone(NodeConfig::new(1, "/d"), RunMode::Stream);
        let read_back: ServiceConfig = parse(
            "mode \"standalone\"\nnode id=1 data_dir=\"/d\"\nrun \"stream\"\nworkflow id=\"w\"\n",
        )
        .expect("parses");
        assert_eq!(
            format!(
                "{:?}",
                ServiceConfig {
                    workflows: read_back.workflows.clone(),
                    ..standalone
                }
            ),
            format!("{read_back:?}"),
        );
    }

    #[test]
    fn a_store_block_is_written_the_way_it_is_read() {
        for batch_resume in [false, true] {
            let mut config = parse(&minimal_standalone_kdl()).expect("parses");
            config.store = Some(StoreConfig::Redb {
                path: PathBuf::from("/var/lib/saci/state.redb"),
                batch_resume,
            });
            let written = config.to_kdl().expect("writes");
            assert!(
                written.contains(&format!(
                    "store \"redb\" path=\"/var/lib/saci/state.redb\"{}\n",
                    if batch_resume {
                        " batch_resume=#true"
                    } else {
                        ""
                    }
                )),
                "{written}"
            );
            let reread = parse(&written).expect("reads back");
            assert_eq!(as_json(&reread), as_json(&config));
        }
    }

    #[test]
    fn a_block_writes_only_the_fields_that_differ_from_the_default() {
        let mut config = parse(&minimal_cluster_kdl()).expect("parses");
        config.http.control = false;
        config.observability.log_level = "debug".to_string();

        let written = config.to_kdl().expect("writes");
        assert!(
            written.contains(
                "\ncluster bootstrap=#true {\n  peer id=1 addr=\"127.0.0.1:9000\"\n  \
                 peer id=2 addr=\"127.0.0.2:9000\"\n}\n"
            ),
            "{written}"
        );
        assert!(written.contains("\nhttp control=#false\n"), "{written}");
        assert!(
            written.contains("\nobservability log_level=\"debug\"\n"),
            "{written}"
        );
        assert_eq!(
            as_json(&parse(&written).expect("reads back")),
            as_json(&config)
        );
    }
}
