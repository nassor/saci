//! The `http` control-plane block.

use serde::{Deserialize, Serialize};

fn default_http_bind() -> String {
    "0.0.0.0:8080".to_string()
}

fn default_http_control() -> bool {
    true
}

fn default_http_enabled() -> bool {
    true
}

// `skip_serializing_if` predicates: a field at its default is not written.
fn is_default_bind(value: &str) -> bool {
    value == default_http_bind()
}
fn is_true(value: &bool) -> bool {
    *value
}

/// HTTP control-plane configuration.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct HttpConfig {
    /// Socket address to bind the HTTP server on.
    #[serde(default = "default_http_bind", skip_serializing_if = "is_default_bind")]
    pub bind: String,
    /// Serve the HTTP control plane. `enabled=#false` turns it off entirely.
    #[serde(default = "default_http_enabled", skip_serializing_if = "is_true")]
    pub enabled: bool,
    /// Mount the workflow lifecycle endpoints (`/api/workflows*`).
    ///
    /// On by default, for the same reason `observability.inspector.enabled`
    /// and `.ui` are: this port already serves the topology, the connector
    /// option allowlist and the whole log tail unauthenticated, so the
    /// exposure posture is unchanged. `control #false` is the documented
    /// opt-out and drops the routes from the router entirely. Standalone mode
    /// only: a cluster node declares one workflow and stopping it is stopping
    /// the node, so the routes are never mounted there.
    #[serde(default = "default_http_control", skip_serializing_if = "is_true")]
    pub control: bool,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            bind: default_http_bind(),
            enabled: default_http_enabled(),
            control: default_http_control(),
        }
    }
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use crate::service::config::test_support::{TRIVIAL_WORKFLOW, minimal_standalone_kdl, parse};

    #[test]
    fn test_http_disabled_skips_bind_validation() {
        let raw = format!(
            r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

http bind="not-a-socket-addr" enabled=#false
{TRIVIAL_WORKFLOW}
"#
        );
        let cfg = parse(&raw).expect("parse");
        assert!(!cfg.http.enabled);
        cfg.validate()
            .expect("a disabled http plane should not validate bind");
    }

    #[test]
    fn test_http_disabled_key_is_refused() {
        let raw = format!(
            "{}\nhttp bind=\"127.0.0.1:8080\" disabled=#true\n",
            minimal_standalone_kdl()
        );
        let err = parse(&raw).expect_err("`disabled` was replaced by `enabled`");
        assert!(err.contains("disabled"), "{err}");
    }
}
