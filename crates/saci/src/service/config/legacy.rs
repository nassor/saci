//! Hints for the spellings of the v0.2.0 config language.
//!
//! The old keys are still refused: every spec type rejects an unknown key, so
//! a v0.2.0 file fails on its first one with serde's "unknown field". This
//! module only reads the parsed document a second time, after that failure,
//! and names what each old spelling became, so the operator fixes the whole
//! file in one pass instead of one error at a time.
//!
//! The walk is positional: a key is reported only where v0.2.0 gave it its
//! old meaning. A processor's `config` map or a connector's `config` bag holds
//! keys the service does not know, and `module` or `schema_fields` there is
//! the connector's own business. Those bag keys are additionally reported only
//! when the file already shows a spelling that no current build accepts, so a
//! custom connector that keeps a `schema_fields` key of its own never earns a
//! hint for an unrelated error.

use crate::config::{ConfigMap, ConfigValue};

/// A v0.2.0 `type=` value and the connector name it is registered under now.
const CONNECTORS: &[(&str, &str)] = &[
    ("ChannelSource", "channel"),
    ("ChannelSink", "channel"),
    ("ClickhouseSource", "clickhouse"),
    ("ClickhouseSink", "clickhouse"),
    ("FileSource", "file"),
    ("FileSink", "file"),
    ("HttpSource", "http"),
    ("HttpSink", "http"),
    ("HttpPushSource", "http-listen"),
    ("KafkaSource", "kafka"),
    ("KafkaSink", "kafka"),
    ("MariadbSource", "mariadb"),
    ("MariadbSink", "mariadb"),
    ("NatsSource", "nats"),
    ("NatsSink", "nats"),
    ("OpenTelemetryMetricsSource", "otlp-metrics"),
    ("OpenTelemetryMetricsSink", "otlp-metrics"),
    ("PostgresSource", "postgresql"),
    ("PostgresSink", "postgresql"),
    ("RedbSource", "redb"),
    ("RedbSink", "redb"),
    ("S3Source", "s3"),
    ("S3Sink", "s3"),
    ("saci", "peer"),
    ("tcp", "tcp"),
    ("TursoSource", "turso"),
    ("TursoSink", "turso"),
    ("udp", "udp"),
];

/// The cluster settings v0.2.0 wrote at the top of the document.
const CLUSTER_KEYS: &[&str] = &[
    "bootstrap",
    "peer",
    "lease_ttl_ms",
    "election_timeout_ms",
    "heartbeat_interval_ms",
    "snapshot_log_interval",
];

/// One hint per v0.2.0 spelling found in `config`, the document as parsed
/// (before variable substitution).
///
/// Empty when the document uses none of them: only a file that is evidently
/// written for v0.2.0 gets hints, and then also for the opaque `config` keys
/// that were renamed with it.
pub(super) fn hints(config: &ConfigValue) -> Vec<String> {
    let Some(root) = config.as_object() else {
        return Vec::new();
    };

    let mut structural = Vec::new();
    let mut bags = Vec::new();

    if let Some(run_mode) = root.get("run_mode") {
        structural.push(run_mode_hint(run_mode));
    }

    let cluster: Vec<String> = CLUSTER_KEYS
        .iter()
        .filter(|key| root.contains_key(**key))
        .map(|key| format!("`{key}`"))
        .collect();
    if !cluster.is_empty() {
        structural.push(format!(
            "{} no longer sit at the top of the document: every cluster key lives in one \
             `cluster {{ }}` block",
            cluster.join(", ")
        ));
    }

    if let Some(http) = root.get("http").and_then(ConfigValue::as_object)
        && let Some(disabled) = http.get("disabled")
    {
        structural.push(match disabled.as_bool() {
            Some(flag) => format!("`http disabled=#{flag}` is now `http enabled=#{}`", !flag),
            None => {
                "`http disabled=..` is now `http enabled=..`, with the opposite sense".to_string()
            }
        });
    }

    if let Some(inspector) = root
        .get("observability")
        .and_then(ConfigValue::as_object)
        .and_then(|observability| observability.get("inspector"))
        .and_then(ConfigValue::as_object)
    {
        for (old, new) in [
            ("retention_secs", "retention_ms"),
            ("sample_interval_secs", "sample_interval_ms"),
        ] {
            if inspector.contains_key(old) {
                structural.push(format!(
                    "observability inspector: `{old}` is now `{new}`, in milliseconds"
                ));
            }
        }
    }

    for workflow in tables(root.get("workflow")) {
        workflow_hints(workflow, &mut structural, &mut bags);
    }

    if structural.is_empty() {
        return structural;
    }
    structural.append(&mut bags);
    structural
}

fn run_mode_hint(run_mode: &ConfigValue) -> String {
    let table = run_mode.as_object();
    let kind = table
        .and_then(|table| table.get("kind"))
        .and_then(ConfigValue::as_str);
    match kind {
        Some("one_shot") => r#"`run_mode kind="one_shot"` is now `run "once"`"#.to_string(),
        Some("continuous") => r#"`run_mode kind="continuous"` is now `run "continuous"`"#.to_string(),
        Some("stream") => r#"`run_mode kind="stream"` is now `run "stream"`"#.to_string(),
        Some("interval") => {
            match table
                .and_then(|table| table.get("interval_ms"))
                .and_then(ConfigValue::as_u64)
            {
                Some(ms) => format!(
                    r#"`run_mode kind="interval" interval_ms={ms}` is now `run "interval" every_ms={ms}`"#
                ),
                None => r#"`run_mode kind="interval" interval_ms=N` is now `run "interval" every_ms=N`"#
                    .to_string(),
            }
        }
        _ => r#"`run_mode kind=..` is now `run "once"`, `run "continuous"`, `run "stream"` or `run "interval" every_ms=N`"#
            .to_string(),
    }
}

fn workflow_hints(workflow: &ConfigMap, structural: &mut Vec<String>, bags: &mut Vec<String>) {
    let workflow_label = label("workflow", workflow);

    if let Some(transformers) = workflow.get("transformer") {
        structural.push(
            if tables(Some(transformers)).any(|table| table.contains_key("options")) {
                format!(
                    "{workflow_label}: `transformer \"id\" format=.. {{ options .. }}` is now \
                     `codec \"id\" format=.. {{ config .. }}`"
                )
            } else {
                format!(
                    "{workflow_label}: `transformer \"id\" format=..` is now \
                     `codec \"id\" format=..`"
                )
            },
        );
    }

    for kind in ["source", "sink"] {
        for node in tables(workflow.get(kind)) {
            let scope = format!("{workflow_label} {}", label(kind, node));
            let connector = connector_hint(&scope, node, structural);

            if let Some(transformer) = node.get("transformer") {
                structural.push(match transformer.as_str() {
                    Some(id) => {
                        format!("{scope}: `transformer=\"{id}\"` is now `codec=\"{id}\"`")
                    }
                    None => format!("{scope}: `transformer=..` is now `codec=..`"),
                });
            }

            if let Some(config) = node.get("config").and_then(ConfigValue::as_object) {
                bag_hints(&scope, kind, connector, config, bags);
            }
        }
    }

    for (kind, old) in [("wasm", "module"), ("plugin", "library")] {
        for node in tables(workflow.get(kind)) {
            let scope = format!("{workflow_label} {}", label(kind, node));
            if node.contains_key(old) {
                structural.push(format!("{scope}: `{old}=` is now `path=`"));
            }
            if node
                .get("window")
                .and_then(ConfigValue::as_object)
                .is_some_and(|window| window.contains_key("key_field"))
            {
                structural.push(format!("{scope} window: `key_field` is now `key_fields`"));
            }
        }
    }
}

/// Report a source or sink's `type=`; returns the connector it names now, when
/// the old name is one of the built-in ones.
fn connector_hint(
    scope: &str,
    node: &ConfigMap,
    structural: &mut Vec<String>,
) -> Option<&'static str> {
    let old = node.get("type")?;
    let known = old
        .as_str()
        .and_then(|name| CONNECTORS.iter().find(|(old_name, _)| *old_name == name));
    match known {
        Some(&(old_name, connector)) => {
            structural.push(format!(
                "{scope}: `type=\"{old_name}\"` is now `connector=\"{connector}\"`"
            ));
            Some(connector)
        }
        None => {
            structural.push(format!(
                "{scope}: `type=` is now `connector=`, which takes the lowercase name the \
                 connector registers under (`file`, `kafka`, `nats`, ...)"
            ));
            None
        }
    }
}

/// The renamed keys inside a source or sink's opaque `config`.
fn bag_hints(
    scope: &str,
    kind: &str,
    connector: Option<&str>,
    config: &ConfigMap,
    bags: &mut Vec<String>,
) {
    if let Some(fields) = config.get("schema_fields") {
        let native = tables(Some(fields))
            .any(|field| field.contains_key("pg_type") || field.contains_key("mariadb_type"));
        bags.push(format!(
            "{scope} config: `schema_fields` is now `field`, written with lowercase type names \
             (`int64`, `utf8`, ...){}",
            if native {
                "; `pg_type` and `mariadb_type` are `native_type`"
            } else {
                ""
            }
        ));
    }
    if kind == "source"
        && matches!(connector, Some("kafka" | "nats"))
        && config.contains_key("batch_size")
    {
        bags.push(format!("{scope} config: `batch_size` is now `batch_rows`"));
    }
    if matches!(
        config.get("schema_from").and_then(ConfigValue::as_str),
        Some("body" | "object")
    ) {
        bags.push(format!(
            "{scope} config: `schema_from` \"body\" and \"object\" are now \"payload\""
        ));
    }
}

/// The tables under a key that may hold one table or an array of them.
fn tables(value: Option<&ConfigValue>) -> impl Iterator<Item = &ConfigMap> {
    let items: &[ConfigValue] = match value {
        Some(ConfigValue::Array(items)) => items,
        Some(single) => std::slice::from_ref(single),
        None => &[],
    };
    items.iter().filter_map(ConfigValue::as_object)
}

/// `source "orders"`, or just `source` for a node with no string id.
fn label(kind: &str, node: &ConfigMap) -> String {
    match node.get("id").and_then(ConfigValue::as_str) {
        Some(id) => format!("{kind} \"{id}\""),
        None => kind.to_string(),
    }
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;
    use crate::config::from_kdl_str;
    use crate::service::ServiceConfig;

    fn hints_of(text: &str) -> Vec<String> {
        hints(&from_kdl_str(text).expect("valid KDL"))
    }

    /// A v0.2.0 file, as `examples/configs` wrote them: every renamed spelling
    /// at once.
    const V020: &str = r#"
mode "standalone"
node id=1 data_dir="/tmp/saci"
run_mode kind="interval" interval_ms=5000

workflow "orders" {
    transformer "csv_fmt" format="csv" {
        options has_headers=#true
    }
    source "csv_orders" type="FileSource" component="Order" transformer="csv_fmt" {
        config {
            path "orders.csv"
            schema_fields "id" type="Int64" nullable=#false
        }
    }
    wasm "p" module="p.wasm" {
        window kind="tumbling" size_ms=1000 time_field="ts" {
            key_field "region"
        }
    }
    sink "out" type="PostgresSink" component="Order" {
        config {
            schema_fields "id" type="int64" pg_type="bigint"
        }
    }
    link from="csv_orders" to="p"
    link from="p" to="out"
}

http bind="0.0.0.0:8080" disabled=#true

observability log_level="info" {
    inspector retention_secs=60 sample_interval_secs=1
}
"#;

    #[test]
    fn a_v020_file_is_refused_with_a_hint_for_every_old_spelling() {
        let err = ServiceConfig::from_kdl(V020).expect_err("the old spelling is still refused");
        assert_eq!(err.category(), "configuration");
        let message = err.message();

        assert!(message.starts_with("parsing config: "), "{message}");
        for expected in [
            r#"hint: `run_mode kind="interval" interval_ms=5000` is now `run "interval" every_ms=5000`"#,
            r#"hint: workflow "orders": `transformer "id" format=.. { options .. }` is now `codec "id" format=.. { config .. }`"#,
            r#"hint: workflow "orders" source "csv_orders": `type="FileSource"` is now `connector="file"`"#,
            r#"hint: workflow "orders" source "csv_orders": `transformer="csv_fmt"` is now `codec="csv_fmt"`"#,
            r#"hint: workflow "orders" sink "out": `type="PostgresSink"` is now `connector="postgresql"`"#,
            r#"hint: workflow "orders" wasm "p": `module=` is now `path=`"#,
            r#"hint: workflow "orders" wasm "p" window: `key_field` is now `key_fields`"#,
            r#"hint: `http disabled=#true` is now `http enabled=#false`"#,
            r#"hint: observability inspector: `retention_secs` is now `retention_ms`, in milliseconds"#,
            r#"hint: observability inspector: `sample_interval_secs` is now `sample_interval_ms`, in milliseconds"#,
            r#"workflow "orders" source "csv_orders" config: `schema_fields` is now `field`"#,
            r#"workflow "orders" sink "out" config: `schema_fields` is now `field`, written with lowercase type names (`int64`, `utf8`, ...); `pg_type` and `mariadb_type` are `native_type`"#,
        ] {
            assert!(
                message.contains(expected),
                "missing {expected:?} in:\n{message}"
            );
        }
    }

    #[test]
    fn the_old_spelling_is_still_rejected_without_the_hints() {
        // The deserializers decide; the hints only decorate their error.
        let value = from_kdl_str(V020).expect("valid KDL");
        let err = <ServiceConfig as serde::Deserialize>::deserialize(value)
            .expect_err("v0.2.0 spelling")
            .to_string();
        assert!(
            err.contains("run_mode") || err.contains("unknown field"),
            "{err}"
        );
        assert!(!err.contains("hint:"), "{err}");
    }

    #[test]
    fn a_plugin_library_key_and_the_old_cluster_layout_are_named() {
        let found = hints_of(
            r#"
mode "cluster"
bootstrap #true
peer id=1 addr="a:1"
peer id=2 addr="b:2"
lease_ttl_ms 30000
workflow "w" {
    plugin "p" library="libp.so"
}
"#,
        );
        assert_eq!(
            found,
            [
                "`bootstrap`, `peer`, `lease_ttl_ms` no longer sit at the top of the document: \
                 every cluster key lives in one `cluster { }` block",
                r#"workflow "w" plugin "p": `library=` is now `path=`"#,
            ]
        );
    }

    #[test]
    fn each_run_mode_kind_names_its_replacement() {
        for (old, new) in [
            (r#"kind="one_shot""#, r#"run "once""#),
            (r#"kind="continuous""#, r#"run "continuous""#),
            (r#"kind="stream""#, r#"run "stream""#),
        ] {
            let found = hints_of(&format!("run_mode {old}\n"));
            assert_eq!(found, [format!("`run_mode {old}` is now `{new}`")]);
        }
    }

    #[test]
    fn a_type_that_is_not_a_built_in_connector_still_names_the_new_key() {
        let found = hints_of(
            r#"
workflow "w" {
    source "m" type="MongoSource" component="orders"
}
"#,
        );
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(
            found[0].starts_with(
                r#"workflow "w" source "m": `type=` is now `connector=`, which takes the lowercase name"#
            ),
            "{found:?}"
        );
    }

    #[test]
    fn a_kafka_source_batch_size_and_an_http_schema_from_are_named() {
        let found = hints_of(
            r#"
workflow "w" {
    source "k" type="KafkaSource" component="c" {
        config {
            batch_size 500
        }
    }
    source "h" type="HttpSource" component="c" {
        config {
            schema_from "body"
        }
    }
}
"#,
        );
        assert!(
            found.contains(
                &r#"workflow "w" source "k" config: `batch_size` is now `batch_rows`"#.to_string()
            ),
            "{found:?}"
        );
        assert!(
            found.contains(
                &r#"workflow "w" source "h" config: `schema_from` "body" and "object" are now "payload""#
                    .to_string()
            ),
            "{found:?}"
        );
    }

    #[test]
    fn a_current_document_gets_no_hints() {
        let found = hints_of(
            r#"
mode "standalone"
node id=1 data_dir="/tmp/saci"
run "once"
workflow "w" {
    codec "c" format="csv" {
        config has_headers=#true
    }
    source "in" connector="file" component="X" codec="c" {
        config {
            path "in.csv"
            field "id" type="int64"
        }
    }
    wasm "p" path="p.wasm" {
        config module="kept" library="kept"
        window kind="session" gap_ms=10 time_field="ts"
    }
    sink "out" connector="file" component="X"
}
http enabled=#false
"#,
        );
        assert!(found.is_empty(), "{found:?}");
    }

    #[test]
    fn a_connector_keeping_a_key_of_its_own_earns_no_hint_for_an_unrelated_error() {
        // No structural old spelling, so the opaque `schema_fields` is the
        // connector's own and the file's real error stands alone.
        let found = hints_of(
            r#"
workflow "w" {
    source "m" connector="mongodb" component="orders" {
        config {
            schema_fields "id" type="int64"
        }
    }
}
"#,
        );
        assert!(found.is_empty(), "{found:?}");
    }

    #[test]
    fn a_failure_without_an_old_spelling_keeps_its_plain_message() {
        let err = ServiceConfig::from_kdl(
            "mode \"standalone\"\nnode id=1 data_dir=\"/tmp/saci\"\nhtpp bind=\"x\"\n",
        )
        .expect_err("a misspelled key");
        assert!(!err.message().contains("hint:"), "{}", err.message());
    }
}
