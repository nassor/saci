//! The run mode: the `mode` node with the `run` node or `cluster` block that
//! belongs to it.

use serde::{Deserialize, Serialize};

use super::flexible::de_bool_flexible;
use crate::config::{ConfigMap, ConfigValue, one_or_many};

/// How the standalone service drives pipeline execution.
///
/// Written as the `run` node: its argument is the kind, and `interval` and
/// `cron` take properties.
///
/// ```kdl
/// run "continuous"
/// run "once"
/// run "interval" every_ms=5000
/// run "cron" expr="0 */15 * * *" timezone="America/New_York"
/// run "stream"
/// ```
///
/// The KDL parser stores a node's leading argument under `id`, so
/// `run "stream"` reaches the deserializer as the bare string and
/// `run "interval" every_ms=5000` as a table holding `id` and `every_ms`.
/// Both shapes are read here, and `Serialize` writes them back the same way.
#[derive(Debug, Clone, Default, PartialEq)]
pub enum RunMode {
    /// Run continuously, re-entering the pipeline as the source produces work.
    ///
    /// A pass that drained every source (or admitted nothing) waits 100 ms
    /// before the next; one that spent its flow-control credit with the source
    /// still live, or left a carry-over slice, re-enters immediately.
    #[default]
    Continuous,
    /// Run the pipeline exactly once, then exit.
    Once,
    /// Re-run the pipeline every `every_ms` milliseconds while its sources
    /// are drained.
    Interval {
        /// Milliseconds between successive pipeline runs.
        ///
        /// The idle poll cadence, not a throughput cap: an iteration that
        /// spent its flow-control credit with the source still live, or that
        /// left a carry-over slice, re-enters immediately and this wait is
        /// skipped. Only a pass that drained every source pays it.
        every_ms: u64,
    },
    /// Re-run the pipeline on a cron schedule, optionally evaluated in a
    /// named IANA timezone or fixed UTC offset.
    Cron {
        /// Cron expression (5-field, 6-field, or `@hourly`/`@daily`/etc.).
        expr: String,
        /// Optional IANA timezone or fixed offset (`+HH:MM`/`-HH:MM`).
        /// Defaults to UTC when absent.
        timezone: Option<String>,
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

impl<'de> Deserialize<'de> for RunMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error as _;

        const KINDS: &str = r#""once", "continuous", "interval", "cron" or "stream""#;

        let (kind, mut properties) = match ConfigValue::deserialize(deserializer)? {
            ConfigValue::String(kind) => (kind, ConfigMap::new()),
            ConfigValue::Object(mut table) => match table.remove("id") {
                Some(ConfigValue::String(kind)) => (kind, table),
                _ => return Err(D::Error::custom(format!("run needs a kind: {KINDS}"))),
            },
            _ => return Err(D::Error::custom(format!("run needs a kind: {KINDS}"))),
        };
        let every_ms = properties.remove("every_ms");
        let expr = properties.remove("expr");
        let schedule = properties.remove("schedule");
        let timezone = properties.remove("timezone");
        if let Some(key) = properties.keys().next() {
            return Err(D::Error::custom(format!(
                "run \"{kind}\": unknown key '{key}'"
            )));
        }
        match kind.as_str() {
            "continuous" | "once" | "stream" => {
                if every_ms.is_some() {
                    return Err(D::Error::custom(format!(
                        "run \"{kind}\" takes no every_ms; only \"interval\" does"
                    )));
                }
                if expr.is_some() || schedule.is_some() || timezone.is_some() {
                    return Err(D::Error::custom(format!(
                        "run \"{kind}\" takes no cron keys; only \"cron\" does"
                    )));
                }
                Ok(match kind.as_str() {
                    "continuous" => Self::Continuous,
                    "once" => Self::Once,
                    _ => Self::Stream,
                })
            }
            "interval" => {
                if expr.is_some() || schedule.is_some() || timezone.is_some() {
                    return Err(D::Error::custom(
                        r#"run "interval" takes no cron keys; only "cron" does"#,
                    ));
                }
                let every_ms =
                    every_ms.ok_or_else(|| D::Error::custom(r#"run "interval" needs every_ms"#))?;
                let every_ms = every_ms.as_u64().ok_or_else(|| {
                    D::Error::custom(format!(
                        "run \"interval\": every_ms must be a non-negative integer, got {every_ms}"
                    ))
                })?;
                Ok(Self::Interval { every_ms })
            }
            "cron" => {
                if every_ms.is_some() {
                    return Err(D::Error::custom(
                        r#"run "cron" takes no every_ms; use expr="..." instead"#,
                    ));
                }
                let raw_expr = match (expr, schedule) {
                    (Some(_), Some(_)) => {
                        return Err(D::Error::custom(
                            r#"run "cron": specify either expr or schedule, not both"#,
                        ));
                    }
                    (Some(v), None) | (None, Some(v)) => v,
                    (None, None) => {
                        return Err(D::Error::custom(r#"run "cron" needs expr="...""#));
                    }
                };
                let expr = raw_expr
                    .as_str()
                    .ok_or_else(|| {
                        D::Error::custom(format!(
                            "run \"cron\": expr must be a string, got {raw_expr}"
                        ))
                    })?
                    .to_string();
                let timezone = match timezone {
                    Some(tz) => Some(
                        tz.as_str()
                            .ok_or_else(|| {
                                D::Error::custom(format!(
                                    "run \"cron\": timezone must be a string, got {tz}"
                                ))
                            })?
                            .to_string(),
                    ),
                    None => None,
                };
                super::cron::CronSchedule::parse(&expr, timezone.as_deref())
                    .map_err(|e| D::Error::custom(format!("run \"cron\": {e}")))?;
                Ok(Self::Cron { expr, timezone })
            }
            other => Err(D::Error::custom(format!(
                "unknown run kind '{other}' (expected {KINDS})"
            ))),
        }
    }
}

impl Serialize for RunMode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap as _;

        match self {
            Self::Continuous => serializer.serialize_str("continuous"),
            Self::Once => serializer.serialize_str("once"),
            Self::Stream => serializer.serialize_str("stream"),
            Self::Interval { every_ms } => {
                let mut table = serializer.serialize_map(Some(2))?;
                table.serialize_entry("id", "interval")?;
                table.serialize_entry("every_ms", every_ms)?;
                table.end()
            }
            Self::Cron { expr, timezone } => {
                let len = if timezone.is_some() { 3 } else { 2 };
                let mut table = serializer.serialize_map(Some(len))?;
                table.serialize_entry("id", "cron")?;
                table.serialize_entry("expr", expr)?;
                if let Some(tz) = timezone {
                    table.serialize_entry("timezone", tz)?;
                }
                table.end()
            }
        }
    }
}

/// Configuration for standalone (single-node) mode.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct StandaloneConfig {
    /// Determines how the service drives pipeline runs: the `run` node.
    #[serde(default)]
    pub run: RunMode,
}

/// A peer in the Raft cluster.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
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

// `skip_serializing_if` predicates: a field at its default is not written.
fn is_false(value: &bool) -> bool {
    !*value
}
fn is_default_election_timeout(value: &u64) -> bool {
    *value == default_election_timeout()
}
fn is_default_heartbeat_interval(value: &u64) -> bool {
    *value == default_heartbeat_interval()
}
fn is_default_snapshot_log_interval(value: &u64) -> bool {
    *value == default_snapshot_log_interval()
}
fn is_default_lease_ttl(value: &u64) -> bool {
    *value == default_lease_ttl()
}

/// Configuration for cluster (multi-node Raft) mode: the `cluster { }` block.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
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
    #[serde(
        default,
        deserialize_with = "de_bool_flexible",
        skip_serializing_if = "is_false"
    )]
    pub bootstrap: bool,
    /// How long a claimed row-range lease is held before it may be reclaimed.
    ///
    /// Must be at least three election timeouts: a batch lease has to outlive
    /// one SACI election, or a leader change alone would let a second node
    /// claim a range that is still being processed.
    #[serde(
        default = "default_lease_ttl",
        skip_serializing_if = "is_default_lease_ttl"
    )]
    pub lease_ttl_ms: u64,
    /// Raft election timeout in milliseconds.
    #[serde(
        default = "default_election_timeout",
        skip_serializing_if = "is_default_election_timeout"
    )]
    pub election_timeout_ms: u64,
    /// Raft heartbeat interval in milliseconds.
    #[serde(
        default = "default_heartbeat_interval",
        skip_serializing_if = "is_default_heartbeat_interval"
    )]
    pub heartbeat_interval_ms: u64,
    /// Take a snapshot, and purge the log behind it, every N committed log
    /// entries. Sets the Raft snapshot policy; must be at least 1, because a
    /// zero interval would snapshot on every entry.
    #[serde(
        default = "default_snapshot_log_interval",
        skip_serializing_if = "is_default_snapshot_log_interval"
    )]
    pub snapshot_log_interval: u64,
}

/// Which runtime mode the service runs in.
///
/// The document spells it as the `mode` node plus the block that belongs to
/// it:
///
/// ```kdl
/// mode "standalone"
/// run "interval" every_ms=5000
/// ```
///
/// ```kdl
/// mode "cluster"
/// cluster {
///     bootstrap #true
///     peer id=1 addr="127.0.0.1:9000"
///     peer id=2 addr="127.0.0.2:9000"
/// }
/// ```
///
/// `mode "standalone"` takes an optional `run` node and no `cluster` block;
/// `mode "cluster"` requires the `cluster` block and takes no `run` node.
#[derive(Debug, Clone)]
pub enum ServiceMode {
    /// Single-node operation. No consensus required.
    Standalone {
        /// Standalone-specific options: the `run` node.
        config: StandaloneConfig,
    },
    /// Multi-node Raft cluster.
    Cluster {
        /// Cluster-specific options: the `cluster { }` block.
        config: ClusterConfig,
    },
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;
    use crate::service::config::test_support::{
        TRIVIAL_WORKFLOW, minimal_cluster_kdl, minimal_standalone_kdl, parse,
    };

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

cluster {{
    peer id=1 addr="127.0.0.1:9000"
}}
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

    /// Read a document's `run` node as a [`RunMode`], the way
    /// `ServiceConfig` reads it.
    fn parse_run(raw: &str) -> Result<RunMode, String> {
        let value = crate::config::from_kdl_str(raw).map_err(|e| e.to_string())?;
        let run = value.get("run").expect("document has a run node").clone();
        RunMode::deserialize(run).map_err(|e| e.to_string())
    }

    #[test]
    fn test_run_mode_interval_round_trip() {
        let restored = parse_run(r#"run "interval" every_ms=3000"#).expect("deserialize");
        assert_eq!(restored, RunMode::Interval { every_ms: 3_000 });

        let written = serde_json::to_value(&restored).expect("serialize");
        assert_eq!(
            written,
            serde_json::json!({ "id": "interval", "every_ms": 3000 })
        );
        assert_eq!(RunMode::deserialize(written).expect("read back"), restored);
    }

    #[test]
    fn test_run_mode_kinds_serialize_as_their_bare_name() {
        for (mode, name) in [
            (RunMode::Continuous, "continuous"),
            (RunMode::Once, "once"),
            (RunMode::Stream, "stream"),
        ] {
            let written = serde_json::to_value(&mode).expect("serialize");
            assert_eq!(written, serde_json::json!(name));
            assert_eq!(RunMode::deserialize(written).expect("read back"), mode);
        }
    }

    #[test]
    fn test_run_mode_interval_without_every_ms_is_an_error() {
        let err = parse_run(r#"run "interval""#).expect_err("interval needs a period");
        assert!(
            err.contains("every_ms"),
            "error should name every_ms: {err}"
        );

        let doc = format!("{}\nrun \"interval\"\n", minimal_standalone_kdl());
        let err = parse(&doc).expect_err("the same refusal through the whole document");
        assert!(
            err.contains("every_ms"),
            "error should name every_ms: {err}"
        );
    }

    #[test]
    fn test_run_mode_refuses_what_it_does_not_take() {
        let err = parse_run(r#"run "stream" every_ms=5"#).expect_err("stream has no period");
        assert!(err.contains("every_ms"), "{err}");

        let err = parse_run(r#"run "once" speed=1"#).expect_err("unknown key");
        assert!(err.contains("speed"), "{err}");

        let err = parse_run(r#"run "weekly""#).expect_err("unknown kind");
        assert!(err.contains("weekly"), "{err}");

        let err = parse_run(r#"run "interval" every_ms="soon""#).expect_err("not a number");
        assert!(err.contains("every_ms"), "{err}");
    }

    #[test]
    fn test_run_node_selects_the_standalone_run_mode() {
        let with =
            |run: &str| parse(&format!("{}\n{run}\n", minimal_standalone_kdl())).expect("parse");
        for (text, expected) in [
            (r#"run "once""#, RunMode::Once),
            (r#"run "continuous""#, RunMode::Continuous),
            (
                r#"run "interval" every_ms=250"#,
                RunMode::Interval { every_ms: 250 },
            ),
            (r#"run "stream""#, RunMode::Stream),
        ] {
            match with(text).mode {
                ServiceMode::Standalone { config } => assert_eq!(config.run, expected, "{text}"),
                _ => panic!("expected standalone"),
            }
        }
        match parse(&minimal_standalone_kdl()).expect("parse").mode {
            ServiceMode::Standalone { config } => assert_eq!(config.run, RunMode::Continuous),
            _ => panic!("expected standalone"),
        }
    }

    #[test]
    fn test_a_misspelled_cluster_key_is_an_error_naming_it() {
        let doc = format!(
            "mode \"cluster\"\nnode id=1 data_dir=\"/tmp/saci\"\ncluster {{\n    \
             bootstrap #true\n    quorum 3\n    peer id=1 addr=\"127.0.0.1:9000\"\n}}\n\
             {TRIVIAL_WORKFLOW}"
        );
        let err = parse(&doc).expect_err("an unknown cluster key");
        assert!(err.contains("quorum"), "error should name the key: {err}");
    }

    #[test]
    fn test_run_mode_stream_parses() {
        assert_eq!(parse_run(r#"run "stream""#), Ok(RunMode::Stream));
    }

    #[test]
    fn test_run_mode_cron_round_trip_and_validation() {
        let restored = parse_run(r#"run "cron" expr="*/15 * * * *" timezone="America/New_York""#)
            .expect("deserialize cron");
        assert_eq!(
            restored,
            RunMode::Cron {
                expr: "*/15 * * * *".to_string(),
                timezone: Some("America/New_York".to_string()),
            }
        );
        let written = serde_json::to_value(&restored).expect("serialize");
        assert_eq!(
            written,
            serde_json::json!({
                "id": "cron",
                "expr": "*/15 * * * *",
                "timezone": "America/New_York"
            })
        );
        assert_eq!(RunMode::deserialize(written).expect("read back"), restored);

        let via_alias = parse_run(r#"run "cron" schedule="@daily""#).expect("schedule alias");
        assert_eq!(
            via_alias,
            RunMode::Cron {
                expr: "@daily".to_string(),
                timezone: None,
            }
        );

        assert!(parse_run(r#"run "cron""#).is_err());
        assert!(parse_run(r#"run "cron" expr="invalid""#).is_err());
        assert!(parse_run(r#"run "cron" expr="* * * * *" timezone="Mars/Base""#).is_err());
        assert!(parse_run(r#"run "cron" expr="* * * * *" every_ms=100"#).is_err());
    }
}
