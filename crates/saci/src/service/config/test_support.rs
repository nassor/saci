//! Fixtures shared by the config tests, which live beside the type each one
//! exercises.

use super::*;

/// Parse a fixture the way [`ServiceConfig::load`] does, minus the file
/// read. The error is flattened to a `String` so a test can assert on the
/// text without naming the value tree's error type.
fn parse_as<T: serde::de::DeserializeOwned>(raw: &str) -> Result<T, String> {
    let value = crate::config::from_kdl_str(raw).map_err(|e| e.to_string())?;
    T::deserialize(value).map_err(|e| e.to_string())
}

pub(crate) fn parse(raw: &str) -> Result<ServiceConfig, String> {
    parse_as(raw)
}

/// A trivial, feature-independent one-link workflow: a source straight to
/// a sink. Valid under every WorkflowSpec::validate rule regardless of
/// which of `wasm`/`plugin` are compiled in.
pub(crate) const TRIVIAL_WORKFLOW: &str = r#"
workflow "w" {
    source "in" connector="noop" component="X"
    sink "out" connector="noop" component="X"
    link from="in" to="out"
}
"#;

pub(crate) fn minimal_standalone_kdl() -> String {
    format!(
        r#"
mode "standalone"

node id=1 data_dir="/tmp/saci-test"
{TRIVIAL_WORKFLOW}
"#
    )
}

pub(crate) fn minimal_cluster_kdl() -> String {
    format!(
        r#"
mode "cluster"

node id=1 data_dir="/tmp/saci-cluster"

cluster {{
    bootstrap #true
    peer id=1 addr="127.0.0.1:9000"
    peer id=2 addr="127.0.0.2:9000"
}}
{TRIVIAL_WORKFLOW}
"#
    )
}

pub(crate) fn workflow_kdl(body: &str) -> String {
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

pub(crate) fn workflow_err(body: &str) -> String {
    let cfg = parse(&workflow_kdl(body)).expect("parse should succeed");
    cfg.validate()
        .expect_err("expected a graph validation error")
        .to_string()
}
