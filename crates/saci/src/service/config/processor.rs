//! The `wasm` and `plugin` processor nodes.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::window::WindowConfig;

/// WASM processor node (requires the `wasm-host` feature at runtime).
///
/// ```kdl
/// workflow "w" {
///     wasm "transform" path="pipelines/transform.wasm" sha3_256="abc123..." {
///         config batch_size="1000"
///     }
/// }
/// ```
///
/// `path` is optional: a node declared with no `path` key is a processor
/// whose runtime is supplied programmatically through
/// [`ServiceBuilder::runtime`](crate::service::builder::ServiceBuilder::runtime),
/// looked up by this node's `id`. Building a workflow where such a node has
/// neither a `path` nor a matching registered runtime is a build-time
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Path to the `.wasm` component file (relative or absolute). Absent for
    /// a node whose runtime is supplied through `runtime`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Optional expected SHA3-256 hex digest of the module bytes. Validation
    /// fails at load time if the digest does not match. The value may carry
    /// an optional `sha3-256:` prefix.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha3_256: Option<String>,
    /// Opaque key-value config the processor reads through the
    /// `saci:pipeline/host-io` `get-config` import.
    #[serde(
        default,
        skip_serializing_if = "HashMap::is_empty",
        serialize_with = "super::serialize_sorted"
    )]
    pub config: HashMap<String, String>,
    /// Windowing declaration: how the host merges this node's inbound
    /// streams and tracks its event-time watermark. The geometry is injected
    /// into `config` as `window.*` keys, so the processor's merging logic
    /// reads one source of truth.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<WindowConfig>,
}

impl WasmSpec {
    /// A wasm node with this `id`, no `path`, no digest, an empty `config` and
    /// no `window`.
    ///
    /// With no `path` the runtime must be registered under `id` through
    /// `ServiceBuilder::runtime`; set the public fields for anything else.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: None,
            path: None,
            sha3_256: None,
            config: HashMap::new(),
            window: None,
        }
    }
}

/// Native plugin processor node (requires the `plugin-host` feature at runtime).
///
/// ```kdl
/// workflow "w" {
///     plugin "audit" path="pipelines/libtransform.so" sha3_256="abc123..." {
///         config batch_size="1000"
///     }
/// }
/// ```
///
/// `path` names the shared library the host loads with `dlopen`. It is
/// optional for the same reason [`WasmSpec::path`] is: a node with no `path`
/// relies on a runtime registered under this node's `id` through
/// [`ServiceBuilder::runtime`](crate::service::builder::ServiceBuilder::runtime).
///
/// Unknown keys are rejected: a key the service cannot honour is a
/// configuration error, not something to ignore silently.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct PluginSpec {
    /// Mandatory id, from the node's leading argument. Unique workflow-wide.
    pub id: String,
    /// Optional display name. The dashboard shows the id when this is absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Path to the shared library file (relative or absolute). Absent for a
    /// node whose runtime is supplied through `runtime`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Optional expected SHA3-256 hex digest of the library file bytes.
    /// Validation fails at load time if the digest does not match. The value
    /// may carry an optional `sha3-256:` prefix.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha3_256: Option<String>,
    /// Opaque key-value config the plugin reads through the `get_config`
    /// callback on the host vtable.
    #[serde(
        default,
        skip_serializing_if = "HashMap::is_empty",
        serialize_with = "super::serialize_sorted"
    )]
    pub config: HashMap<String, String>,
    /// Windowing declaration: how the host merges this node's inbound
    /// streams and tracks its event-time watermark. The geometry is injected
    /// into `config` as `window.*` keys, so the plugin's merging logic reads
    /// one source of truth.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<WindowConfig>,
}

impl PluginSpec {
    /// A plugin node with this `id`, no `path`, no digest, an empty `config`
    /// and no `window`.
    ///
    /// With no `path` the runtime must be registered under `id` through
    /// `ServiceBuilder::runtime`; set the public fields for anything else.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: None,
            path: None,
            sha3_256: None,
            config: HashMap::new(),
            window: None,
        }
    }
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use crate::service::config::test_support::parse;

    #[test]
    fn test_processor_nodes_locate_their_artifact_with_path() {
        let doc = |nodes: &str| {
            format!(
                "mode \"standalone\"\nnode id=1 data_dir=\"/tmp/saci\"\nworkflow \"w\" {{\n{nodes}\n}}\n"
            )
        };
        let cfg = parse(&doc(
            "wasm \"a\" path=\"a.wasm\"\nplugin \"b\" path=\"libb.so\"",
        ))
        .expect("parse");
        assert_eq!(cfg.workflows[0].wasm[0].path.as_deref(), Some("a.wasm"));
        assert_eq!(cfg.workflows[0].plugin[0].path.as_deref(), Some("libb.so"));

        for (old, key) in [
            (r#"wasm "a" module="a.wasm""#, "module"),
            (r#"plugin "b" library="libb.so""#, "library"),
        ] {
            let err = parse(&doc(old)).expect_err(old);
            assert!(err.contains(key), "{old}: {err}");
        }
    }

    #[cfg(feature = "wasm-host")]
    #[test]
    fn test_wasm_watch_key_is_rejected() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    wasm "p" path="pipeline.wasm" watch=#true
}
"#;
        let err = parse(raw).expect_err("watch was never implemented; it must not parse");
        assert!(err.contains("watch"), "{err}");
    }

    /// `one_or_many` goes through the value tree rather than an untagged enum
    /// specifically so a `deny_unknown_fields` violation inside one entry
    /// still names the offending key instead of collapsing into a generic
    /// "data did not match any variant" error.
    #[cfg(feature = "wasm-host")]
    #[test]
    fn test_wasm_unknown_key_is_rejected_with_field_name() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    wasm "validate" path="validate.wasm"
    wasm "settle" path="settle.wasm" watch=#true
}
"#;
        let err = parse(raw).expect_err("watch was never implemented; it must not parse");
        assert!(err.contains("watch"), "{err}");
    }

    #[cfg(feature = "plugin-host")]
    #[test]
    fn test_plugin_workflow_round_trips() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    plugin "audit" path="pipelines/libtransform.so" sha3_256="sha3-256:abc123" {
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
        assert_eq!(spec.path.as_deref(), Some("pipelines/libtransform.so"));
        assert_eq!(spec.sha3_256.as_deref(), Some("sha3-256:abc123"));
        assert_eq!(
            spec.config.get("smoketest.multiplier").map(String::as_str),
            Some("10")
        );
        cfg.validate()
            .expect("plugin-only workflow should be valid");
    }

    #[cfg(feature = "plugin-host")]
    #[test]
    fn test_plugin_unknown_key_is_rejected() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    plugin "p" path="libtransform.so" module="libtransform.so"
}
"#;
        let err = parse(raw).expect_err("a key the loader cannot honour must not parse");
        assert!(err.contains("module"), "{err}");
    }

    #[cfg(feature = "plugin-host")]
    #[test]
    fn test_plugin_digest_and_config_default_to_empty() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    plugin "p" path="libtransform.so"
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

    #[cfg(feature = "plugin-host")]
    #[test]
    fn test_plugin_with_no_path_relies_on_a_registered_native_runtime() {
        let raw = r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

workflow "w" {
    plugin "p" name="P"
}
"#;
        let cfg = parse(raw).expect("parse");
        let spec = &cfg.workflows[0].plugin[0];
        assert!(spec.path.is_none());
        cfg.validate()
            .expect("an artifact-less processor entry point is structurally valid");
    }
}
