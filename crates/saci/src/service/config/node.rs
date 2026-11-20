//! The `node` block: identity and storage of one service node.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::flexible::de_u64_flexible;

/// Identity and storage configuration for a single service node.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
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

#[cfg(all(test, feature = "service"))]
mod tests {
    use crate::service::config::test_support::parse;

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
}
