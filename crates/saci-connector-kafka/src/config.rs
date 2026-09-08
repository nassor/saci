//! Serde-derived configuration for the Kafka source and sink.
//!
//! Named keys are values SACI itself interprets. Every librdkafka property goes
//! on the `properties` node and is passed through untouched. There is no
//! second way to set the same thing, and `properties` is applied last, so it
//! overrides any default this connector sets.
//!
//! Both top-level configs carry `#[serde(deny_unknown_fields)]`: a key the
//! connector cannot honour is a configuration error, not something to drop
//! silently. Each exposes `validate`, which the constructors in
//! [`crate::source`](crate) and [`crate::sink`](crate) call before they build
//! anything.

use std::collections::BTreeMap;

use rdkafka::ClientConfig;
use serde::Deserialize;

use saci_connector::ConfigValue;
use saci_core::error::SaciError;

/// How a topic is provisioned before the client uses it.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct TopicProvision {
    /// Create the topic when it does not exist. On by default.
    #[serde(default = "default_true")]
    pub create: bool,
    /// Partition count for a topic this connector creates.
    #[serde(default = "default_partitions")]
    pub partitions: i32,
    /// Replication factor for a topic this connector creates.
    #[serde(default = "default_replication")]
    pub replication_factor: i32,
    /// Broker-side topic config entries, e.g. `"retention.ms"="60000"`.
    #[serde(default)]
    pub config: BTreeMap<String, String>,
    /// Admin operation timeout.
    #[serde(default = "default_provision_timeout_ms")]
    pub timeout_ms: u64,
}

// `Default` must agree with the serde defaults above: omitting the whole
// `provision` node uses `Default`, and that is what makes "create by
// default" true when the user writes nothing.
impl Default for TopicProvision {
    fn default() -> Self {
        Self {
            create: true,
            partitions: default_partitions(),
            replication_factor: default_replication(),
            config: BTreeMap::new(),
            timeout_ms: default_provision_timeout_ms(),
        }
    }
}

/// Configuration for [`KafkaSource`](crate::KafkaSource).
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct KafkaSourceConfig {
    /// Comma-separated bootstrap servers.
    pub brokers: String,
    /// Topic to consume. Comma-separated for several.
    pub topic: String,
    /// Consumer group.
    #[serde(default = "default_group_id")]
    pub group_id: String,
    /// Maximum messages folded into one `RecordBatch`.
    ///
    /// Only in force when the host asks for no size:
    /// `Source::request_batch_rows` overwrites it before every poll, and
    /// `saci-service` sends that hint from the first poll onward unless the
    /// source's `flow_control` block sets `enabled #false`, or the source is
    /// on a path to a windowed node, where the runner withholds the hint so
    /// this value alone decides how large an arrival is.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// How long one `next_batch` keeps collecting after its first message.
    #[serde(default = "default_poll_timeout_ms")]
    pub poll_timeout_ms: u64,
    /// `auto.offset.reset` for a group with no committed offset.
    #[serde(default = "default_auto_offset_reset")]
    pub auto_offset_reset: String,
    /// Commit the previous batch's offsets at the start of the next poll.
    #[serde(default = "default_true")]
    pub commit_on_drain: bool,
    /// Report EOF once every assigned partition is drained, making the source
    /// usable from the batch run modes. Off by default: a Kafka consumer is a
    /// live source.
    #[serde(default)]
    pub stop_at_end: bool,
    /// Column the raw message key is written to. Compacted mode only: the key
    /// is what a snapshot deduplicates on, and it must name a field the
    /// declared schema carries. A source that is not `compacted` decodes the
    /// payload alone and rejects this key rather than ignoring it.
    #[serde(default)]
    pub key_field: Option<String>,
    /// Read the topic as a compacted keyed log: one point-in-time snapshot of
    /// the latest value per key, chunked into `batch_size` batches, then EOF.
    /// Committed group offsets are ignored, because a snapshot is always the
    /// whole state.
    #[serde(default)]
    pub compacted: bool,
    /// Topic provisioning.
    #[serde(default)]
    pub provision: TopicProvision,
    /// librdkafka properties, applied after every default this crate sets.
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
    /// Declared Arrow schema. Parsed by `saci_connector::parse_schema_fields`
    /// from the same table so the type vocabulary matches every other
    /// connector; declared here only so `deny_unknown_fields` accepts the key.
    #[serde(default, deserialize_with = "saci_connector::one_or_many")]
    pub schema_fields: Vec<ConfigValue>,
}

/// Configuration for [`KafkaSink`](crate::KafkaSink).
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct KafkaSinkConfig {
    /// Comma-separated bootstrap servers.
    pub brokers: String,
    /// Topic to produce to. One topic, not a list.
    pub topic: String,
    /// Column whose rendered value becomes the message key. Row-per-message
    /// formats only.
    #[serde(default)]
    pub key_field: Option<String>,
    /// Produce a NULL payload — a Kafka tombstone, the delete marker a
    /// compacted topic reads as "this key is gone" — for a row whose columns
    /// other than `key_field` are all null. Every other row is produced
    /// exactly as it would be with this off.
    #[serde(default)]
    pub tombstones: bool,
    /// How long `finish` waits for the producer queue to drain.
    #[serde(default = "default_flush_timeout_ms")]
    pub flush_timeout_ms: u64,
    /// Topic provisioning.
    #[serde(default)]
    pub provision: TopicProvision,
    /// librdkafka properties, applied after every default this crate sets.
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
    /// See [`KafkaSourceConfig::schema_fields`].
    #[serde(default, deserialize_with = "saci_connector::one_or_many")]
    pub schema_fields: Vec<ConfigValue>,
}

impl KafkaSourceConfig {
    /// Check every cross-field invariant this config must satisfy.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] naming the offending key on the
    /// first violation.
    pub fn validate(&self) -> Result<(), SaciError> {
        let what = "KafkaSource";
        validate_brokers(what, &self.brokers)?;
        validate_topic(what, &self.topic)?;

        if self.batch_size == 0 {
            return Err(SaciError::configuration(format!(
                "{what} config: 'batch_size' must be at least 1"
            )));
        }
        if !matches!(
            self.auto_offset_reset.as_str(),
            "earliest" | "latest" | "none"
        ) {
            return Err(SaciError::configuration(format!(
                "{what} config: 'auto_offset_reset' must be earliest, latest or none"
            )));
        }
        match (self.compacted, self.key_field.as_deref()) {
            (true, None) => {
                return Err(SaciError::configuration(format!(
                    "{what} config: 'compacted' needs 'key_field': a snapshot of a compacted \
                     topic is keyed by the message key"
                )));
            }
            (true, Some(field)) if field.trim().is_empty() => {
                return Err(SaciError::configuration(format!(
                    "{what} config: 'key_field' must name a column"
                )));
            }
            (false, Some(_)) => {
                return Err(SaciError::configuration(format!(
                    "{what} config: 'key_field' is read only with 'compacted' #true"
                )));
            }
            _ => {}
        }

        validate_provision(what, &self.provision)?;
        validate_properties(what, &self.properties)?;
        Ok(())
    }

    /// The subscribed topic names: `topic` split on commas and trimmed.
    pub(crate) fn topics(&self) -> Vec<String> {
        self.topic
            .split(',')
            .map(|t| t.trim().to_string())
            .collect()
    }
}

impl KafkaSinkConfig {
    /// Check every cross-field invariant this config must satisfy.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] naming the offending key on the
    /// first violation.
    pub fn validate(&self) -> Result<(), SaciError> {
        let what = "KafkaSink";
        validate_brokers(what, &self.brokers)?;
        validate_topic(what, &self.topic)?;

        // Whether `key_field` is honourable depends on the format's message
        // shape, which only the resolved transformer knows, so `KafkaSink::new`
        // checks it rather than this method.

        if self.tombstones && self.key_field.is_none() {
            return Err(SaciError::configuration(format!(
                "{what} config: 'tombstones' needs 'key_field': a delete marker names the key \
                 it deletes"
            )));
        }

        validate_provision(what, &self.provision)?;
        validate_properties(what, &self.properties)?;
        Ok(())
    }
}

fn validate_brokers(what: &str, brokers: &str) -> Result<(), SaciError> {
    if brokers.trim().is_empty() {
        return Err(SaciError::configuration(format!(
            "{what} config: 'brokers' must not be empty"
        )));
    }
    Ok(())
}

fn validate_topic(what: &str, topic: &str) -> Result<(), SaciError> {
    if topic.split(',').any(|t| t.trim().is_empty()) {
        return Err(SaciError::configuration(format!(
            "{what} config: 'topic' must name at least one non-empty topic"
        )));
    }
    Ok(())
}

fn validate_provision(what: &str, provision: &TopicProvision) -> Result<(), SaciError> {
    if provision.partitions < 1 {
        return Err(SaciError::configuration(format!(
            "{what} config: 'provision.partitions' must be at least 1"
        )));
    }
    if provision.replication_factor < 1 {
        return Err(SaciError::configuration(format!(
            "{what} config: 'provision.replication_factor' must be at least 1"
        )));
    }
    Ok(())
}

fn validate_properties(what: &str, properties: &BTreeMap<String, String>) -> Result<(), SaciError> {
    if properties.contains_key("bootstrap.servers") {
        return Err(SaciError::configuration(format!(
            "{what} config: set the brokers with 'brokers', not properties.bootstrap.servers"
        )));
    }
    Ok(())
}

/// Build a librdkafka client config: `bootstrap.servers`, then `defaults`,
/// then the user's `properties`, which therefore win.
pub(crate) fn client_config(
    brokers: &str,
    defaults: &[(&str, &str)],
    properties: &BTreeMap<String, String>,
) -> ClientConfig {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", brokers);
    for (key, value) in defaults {
        cfg.set(*key, *value);
    }
    for (key, value) in properties {
        cfg.set(key, value);
    }
    cfg
}

fn default_true() -> bool {
    true
}
fn default_partitions() -> i32 {
    1
}
fn default_replication() -> i32 {
    1
}
fn default_provision_timeout_ms() -> u64 {
    10_000
}
fn default_group_id() -> String {
    "saci".to_string()
}
fn default_batch_size() -> usize {
    1000
}
fn default_poll_timeout_ms() -> u64 {
    1000
}
fn default_auto_offset_reset() -> String {
    "earliest".to_string()
}
fn default_flush_timeout_ms() -> u64 {
    30_000
}

#[cfg(test)]
mod tests {
    use super::*;
    use saci_connector::from_kdl_str;

    fn source(extra: &str) -> KafkaSourceConfig {
        let raw = format!("brokers \"localhost:9092\"\ntopic \"orders\"\n{extra}");
        KafkaSourceConfig::deserialize(from_kdl_str(&raw).expect("parse kdl")).expect("parse")
    }

    fn sink(extra: &str) -> KafkaSinkConfig {
        let raw = format!("brokers \"localhost:9092\"\ntopic \"orders\"\n{extra}");
        KafkaSinkConfig::deserialize(from_kdl_str(&raw).expect("parse kdl")).expect("parse")
    }

    #[test]
    fn source_defaults_are_populated_when_omitted() {
        let cfg = source("");
        assert_eq!(cfg.group_id, "saci");
        assert_eq!(cfg.batch_size, 1000);
        assert_eq!(cfg.poll_timeout_ms, 1000);
        assert_eq!(cfg.auto_offset_reset, "earliest");
        assert!(cfg.commit_on_drain);
        assert!(!cfg.stop_at_end);
        assert_eq!(cfg.key_field, None);
        assert!(!cfg.compacted);
        assert!(cfg.provision.create);
        assert_eq!(cfg.provision.partitions, 1);
        assert_eq!(cfg.provision.replication_factor, 1);
        assert!(cfg.provision.config.is_empty());
        assert_eq!(cfg.provision.timeout_ms, 10_000);
        assert!(cfg.properties.is_empty());
        cfg.validate().expect("defaults are valid");
    }

    #[test]
    fn sink_defaults_are_populated_when_omitted() {
        let cfg = sink("");
        assert_eq!(cfg.key_field, None);
        assert!(!cfg.tombstones);
        assert_eq!(cfg.flush_timeout_ms, 30_000);
        assert!(cfg.provision.create);
        assert!(cfg.properties.is_empty());
        cfg.validate().expect("defaults are valid");
    }

    #[test]
    fn topic_provision_default_creates_by_default() {
        assert!(TopicProvision::default().create);
    }

    #[test]
    fn empty_brokers_is_a_configuration_error() {
        let raw = "brokers \"\"\ntopic \"orders\"\n";
        let cfg =
            KafkaSourceConfig::deserialize(from_kdl_str(raw).expect("parse kdl")).expect("parse");
        let err = cfg.validate().expect_err("empty brokers must fail");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'brokers'"));
    }

    #[test]
    fn empty_topic_is_a_configuration_error() {
        let raw = "brokers \"localhost:9092\"\ntopic \"\"\n";
        let cfg =
            KafkaSourceConfig::deserialize(from_kdl_str(raw).expect("parse kdl")).expect("parse");
        let err = cfg.validate().expect_err("empty topic must fail");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'topic'"));
    }

    #[test]
    fn a_blank_element_in_a_comma_separated_topic_list_is_a_configuration_error() {
        let raw = "brokers \"localhost:9092\"\ntopic \"a,,b\"\n";
        let cfg =
            KafkaSourceConfig::deserialize(from_kdl_str(raw).expect("parse kdl")).expect("parse");
        let err = cfg.validate().expect_err("blank element must fail");
        assert!(err.message().contains("'topic'"));
    }

    #[test]
    fn zero_batch_size_is_a_configuration_error() {
        let cfg = source("batch_size 0\n");
        let err = cfg.validate().expect_err("zero batch_size must fail");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'batch_size'"));
    }

    #[test]
    fn invalid_auto_offset_reset_is_a_configuration_error() {
        let cfg = source("auto_offset_reset \"oldest\"\n");
        let err = cfg
            .validate()
            .expect_err("invalid auto_offset_reset must fail");
        assert!(err.message().contains("'auto_offset_reset'"));
    }

    #[test]
    fn provision_partitions_below_one_is_a_configuration_error() {
        let cfg = source("provision partitions=0\n");
        let err = cfg.validate().expect_err("zero partitions must fail");
        assert!(err.message().contains("'provision.partitions'"));
    }

    #[test]
    fn provision_replication_factor_below_one_is_a_configuration_error() {
        let cfg = source("provision replication_factor=0\n");
        let err = cfg
            .validate()
            .expect_err("zero replication_factor must fail");
        assert!(err.message().contains("'provision.replication_factor'"));
    }

    #[test]
    fn properties_bootstrap_servers_is_a_configuration_error() {
        let cfg = source("properties \"bootstrap.servers\"=\"evil:9092\"\n");
        let err = cfg
            .validate()
            .expect_err("properties.bootstrap.servers must fail");
        assert!(err.message().contains("properties.bootstrap.servers"));
    }

    #[test]
    fn properties_override_a_default_in_client_config() {
        let mut properties = BTreeMap::new();
        properties.insert("auto.offset.reset".to_string(), "latest".to_string());
        let cfg = client_config(
            "localhost:9092",
            &[("auto.offset.reset", "earliest")],
            &properties,
        );
        assert_eq!(cfg.get("auto.offset.reset"), Some("latest"));
        assert_eq!(cfg.get("bootstrap.servers"), Some("localhost:9092"));
    }

    #[test]
    fn client_config_applies_defaults_when_not_overridden() {
        let cfg = client_config(
            "localhost:9092",
            &[("group.id", "saci"), ("enable.auto.commit", "false")],
            &BTreeMap::new(),
        );
        assert_eq!(cfg.get("group.id"), Some("saci"));
        assert_eq!(cfg.get("enable.auto.commit"), Some("false"));
    }

    #[test]
    fn a_compacted_source_parses_its_key_field() {
        let cfg = source("compacted #true\nkey_field \"id\"\n");
        assert!(cfg.compacted);
        assert_eq!(cfg.key_field.as_deref(), Some("id"));
        cfg.validate().expect("a keyed compacted source is valid");
    }

    #[test]
    fn a_compacted_source_without_a_key_field_is_a_configuration_error() {
        let cfg = source("compacted #true\n");
        let err = cfg.validate().expect_err("compacted must name its key");
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("'compacted' needs 'key_field'"),
            "got: {err}"
        );
    }

    #[test]
    fn a_compacted_source_with_a_blank_key_field_is_a_configuration_error() {
        let cfg = source("compacted #true\nkey_field \"  \"\n");
        let err = cfg.validate().expect_err("a blank key_field must fail");
        assert!(err.message().contains("'key_field'"), "got: {err}");
    }

    #[test]
    fn a_key_field_without_compacted_is_a_configuration_error() {
        // Nothing reads the source's key outside compacted mode, and this
        // connector rejects a key it cannot honour rather than dropping it.
        let cfg = source("key_field \"id\"\n");
        let err = cfg.validate().expect_err("an unread key_field must fail");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'compacted' #true"), "got: {err}");
    }

    #[test]
    fn a_tombstone_sink_parses_with_a_key_field() {
        let cfg = sink("tombstones #true\nkey_field \"id\"\n");
        assert!(cfg.tombstones);
        cfg.validate().expect("a keyed tombstone sink is valid");
    }

    #[test]
    fn a_tombstone_sink_without_a_key_field_is_a_configuration_error() {
        let cfg = sink("tombstones #true\n");
        let err = cfg.validate().expect_err("a tombstone must name its key");
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("'tombstones' needs 'key_field'"),
            "got: {err}"
        );
    }
}
